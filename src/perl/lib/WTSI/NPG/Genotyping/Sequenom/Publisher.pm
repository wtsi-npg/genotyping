use utf8;

package WTSI::NPG::Genotyping::Sequenom::Publisher;

use File::Spec;
use File::Temp qw(tempdir);
use List::AllUtils qw(uniq);
use Moose;
use Text::CSV;
use URI;

use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::Publisher;
use WTSI::NPG::iRODS;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable', 'WTSI::NPG::Annotator',
  'WTSI::NPG::Genotyping::Annotator';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'publication_time' =>
  (is       => 'ro',
   isa      => 'DateTime',
   required => 1);

has 'plate_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'sequenom_db' =>
  (is       => 'ro',
   # isa      => 'WTSI::NPG::Genotyping::Database::Sequenom',
   isa      => 'Object',
   required => 1);

has 'snp_db' =>
  (is       => 'ro',
   #  isa      => 'WTSI::NPG::Database::Genotyping::SNP',
   isa      => 'Object',
   required => 1);

has 'ss_warehouse_db' =>
  (is       => 'ro',
   # isa      => 'WTSI::NPG::Database::Warehouse',
   isa      => 'Object',
   required => 1);


sub BUILD {
  my ($self) = @_;

  # Make our iRODS handle use our logger by default
  $self->irods->logger($self->logger);
}

=head2 publish

  Arg [1]    : Str iRODS path that will be the destination for publication
  Arg [2]    : Subset of plate addresses to publish. Optional, defaults to all.

  Example    : $export->publish('/foo', 'A01', 'A02')
  Description: Publish a Sequenom plate to an iRODS path.
  Returntype : Int number of addresses published

=cut

sub publish {
  my ($self, $publish_dest, @addresses) = @_;

  my $num_published = $self->publish_samples($publish_dest);

  return $num_published;
}

=head2 publish_samples

  Arg [1]    : Str iRODS path that will be the destination for publication
  Arg [2]    : Subset of plate addresses to publish. Optional, defaults to all.

  Example    : $export->publish_samples('/foo', 'A01', 'A02')
  Description: Publish the individual samples within a Sequenom plate to an
               iRODS path.
  Returntype : Int number of addresses published

=cut

sub publish_samples {
  my ($self, $publish_dest, @addresses) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);

  my $num_published = 0;
  my $tmpdir = tempdir(CLEANUP => 1);
  my $current_file;

  my $plate = $self->sequenom_db->find_plate_results($self->plate_name);

  unless (@addresses) {
    @addresses = sort keys %$plate;
  }

  my $publisher =
    WTSI::NPG::Publisher->new(irods         => $self->irods,
                              accountee_uid => $self->accountee_uid,
                              logger        => $self->logger);

  my $plate_name;
  my $total = scalar @addresses;
  my $possible = scalar keys %$plate;

  $self->debug("Publishing $total Sequenom CSV data files ",
               "from a possible $possible");

  foreach my $address (@addresses) {
    eval {
      my @records = @{$plate->{$address}};
      my $first = $records[0];
      my @keys = sort keys %$first;

      $plate_name = $first->{plate};
      my $file = sprintf("%s/%s_%s.csv", $tmpdir, $plate_name, $first->{well});
      $current_file = $file;

      my $record_count =
        $self->_write_sequenom_csv_file($file, \@keys, \@records);
      $self->debug("Wrote $record_count records into $file");

      my @meta = $self->make_sequenom_metadata($first);
      my @fingerprint = $self->sequenom_fingerprint(@meta);
      my $rods_path = $publisher->publish_file($file, \@fingerprint,
                                               $publish_dest,
                                               $self->publication_time);

      # Build from local file to avoid and iRODS round trip with iget
      my $resultset = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new
        ($file);
      my $snpset_name = $self->_find_resultset_snpset($resultset);

      $self->debug("Found results to be of SNP set '$snpset_name'");

      my $obj = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
        ($self->irods, $rods_path);
      $obj->add_avu($self->sequenom_plex_name_attr, $snpset_name);

      # Now that adding the secondary metadata is fast enough, we can
      # run it inline here, so that the data are available
      # immediately.
      $obj->update_secondary_metadata($self->snp_db,
                                      $self->ss_warehouse_db);

      unlink $file;
      ++$num_published;
    };

    if ($@) {
      $self->error("Failed to publish '$current_file' to ",
                   "'$publish_dest': ", $@);
    }
    else {
      $self->debug("Published '$current_file': $num_published of $total");
    }
  }

  $self->info("Published $num_published/$total CSV files for '$plate_name' ",
              "to '$publish_dest'");

  return $num_published;
}

# Write to a file subset of data in records that match keys
sub _write_sequenom_csv_file {
  my ($self, $file, $keys, $records) = @_;
  my $records_written = 0;

  # Transform to the required output headers
  my $fn = sub {
    my $x = shift;
    $x =~ '^WELL$'                    && return 'WELL_POSITION';
    $x =~ /^(ASSAY|GENOTYPE|SAMPLE)$/ && return $x . '_ID';
    return $x;
  };

  my @header = map { uc } @$keys;
  @header = map { $fn->($_) } @header;

  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});
  $csv->column_names(\@header);

  # Handle UTF8 because users can enter arbitrary plate names
  open(my $out, '>:encoding(utf8)', $file)
    or $self->logcroak("Failed to open Sequenom CSV file '$file'",
                       " for writing: $!");
  $csv->print($out, \@header)
    or $self->logcroak("Failed to write header [", join(", ", @header),
                       "] to '$file': ", $csv->error_diag);

  foreach my $record (@$records) {
    my @columns;
    foreach my $key (@$keys) {
      push(@columns, $record->{$key});
    }

    $csv->print($out, \@columns)
      or $self->logcroak("Failed to write record [", join(", ", @columns),
                         "] to '$file': ", $csv->error_diag);
    ++$records_written;
  }

  close($out);

  return $records_written;
}

sub _find_resultset_snpset {
  my ($self, $resultset) = @_;

  my @snpset_names;
  foreach my $result (@{$resultset->assay_results}) {
    push @snpset_names, $result->snpset_name;
  }

  @snpset_names = uniq @snpset_names;

  my $num_names = scalar @snpset_names;

  $num_names > 0 or
    $self->logconfess("No SNP set name could be found in '", $resultset->str,
                      "'");
  $num_names == 1 or
    $self->logconfess("$num_names SNP sets found in '", $resultset->str,
                      "': [", join(', ', @snpset_names), "]");

  return shift @snpset_names;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Sequenom::Publisher - An iRODS data publisher
for Sequenom results.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
