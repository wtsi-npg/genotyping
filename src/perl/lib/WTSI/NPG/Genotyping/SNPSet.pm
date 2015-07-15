
use utf8;

package WTSI::NPG::Genotyping::SNPSet;

use List::AllUtils qw(first_value);
use Log::Log4perl::Level;
use Moose;
use Text::CSV;

use WTSI::NPG::Genotyping::GenderMarker;
use WTSI::NPG::Genotyping::Reference;
use WTSI::NPG::Genotyping::SNP;

use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

our @HEADER = qw(SNP_NAME REF_ALLELE ALT_ALLELE CHR POS STRAND);

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::iRODS::Storable',
  'WTSI::NPG::Annotation';

has 'name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   default  => sub { return ''} );

has 'column_names' =>
  (is      => 'ro',
   isa     => 'ArrayRef[Str]',
   writer  => '_write_column_names',
   default => sub { return [@HEADER] });

has 'snps' =>
  (is       => 'ro',
   isa      => ArrayRefOfVariant,
   required => 1,
   builder  => '_build_snps',
   lazy     => 1);

has 'references' =>
  (is       => 'ro',
   isa      => ArrayRefOfReference,
   required => 1,
   builder  => '_build_references',
   lazy     => 1);

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit a DataObject as an anonymous argument mapping to data_object
  # Permit a Str as an anonymous argument mapping to file_name
  if (@args == 1 &&
      ref $args[0] eq 'WTSI::NPG::iRODS::DataObject') {
    return $class->$orig(data_object => $args[0]);
  }
  elsif (@args == 1 and !ref $args[0]) {
    return $class->$orig(file_name => $args[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
  my ($self) = @_;

  my %names;
  foreach my $snp (@{$self->snps}) {
    if (exists $names{$snp->name}) {
      $self->logconfess("Attempted to make a SNPSet containing a duplicate ",
                        "of '", $snp->name, "'");
    }
    else {
      $names{$snp->name} = 1;
      $snp->snpset($self);
    }
  }
}

=head2 snp_names

  Arg [1]    : None

  Example    : $set->snp_names
  Description: Return a sorted array of the names of the SNPs in the set.
  Returntype : Array

=cut

sub snp_names {
  my ($self) = @_;

  my @snp_names = map { $_->name } @{$self->snps};
  my @sorted = sort { $a cmp $b } @snp_names;

  return @sorted;
}

=head2 named_snp

  Arg [1]    : Str SNP name e.g. rs######

  Example    : $snp = $set->named_snp('rs0123456')
  Description: Return specific, named SNP(s) from the set. Raise an
               error if the SNP is not in the set.
  Returntype : WTSI::NPG::Genotyping::SNP

=cut

sub named_snp {
  my ($self, $snp_name) = @_;

  defined $snp_name or
    $self->logconfess("A defined snp_name argument is required");
  $snp_name or
    $self->logconfess("A non-empty snp_name argument is required");

  my $snp = first_value { $_->name eq $snp_name } @{$self->snps};

  $snp or $self->logconfess("SNP set '", $self->name, "' does not contain ",
                            "SNP '$snp_name'");
  return $snp;
}

sub contains_snp {
  my ($self, $snp_name) = @_;

  defined $snp_name or
    $self->logconfess("A defined snp_name argument is required");

  return defined first_value { $_->name eq $snp_name } @{$self->snps};
}

=head2 write_snpset_data

  Arg [1]    : Str file name

  Example    : $set->write_snpset_data('snpset.txt')
  Description: Write the content of the set to a file in the TSV format
               used by NPG. Defaults to the file given by the
               file_name attribute.
  Returntype : Int number of records written (may be > number of unique
               SNP names for cases such as gender markers that have
               multiple locations on the reference genome).

=cut

sub write_snpset_data {
  my ($self, $file_name) = @_;

  $file_name ||= $self->file_name;

  my $records_written = 0;
  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});
  $csv->column_names($self->column_names);

  open my $out, '>:encoding(utf8)', $file_name
    or $self->logcroak("Failed to open SNP set file '$file_name' ",
                       "for writing: $!");

  print $out '#';
  $csv->print($out, $self->column_names);

  foreach my $snp (@{$self->snps}) {
    if (is_GenderMarker($snp)) {
      _write_snp_record($csv, $out, $snp->x_marker) or
        $self->logcroak("Failed to write record [", $snp->x_marker->str,
                        "] to '$file_name': ", $csv->error_diag);
      ++$records_written;

      _write_snp_record($csv, $out, $snp->y_marker) or
        $self->logcroak("Failed to write record [", $snp->y_marker->str,
                        "] to '$file_name': ", $csv->error_diag);
      ++$records_written;
    }
    else {
      _write_snp_record($csv, $out, $snp)
        or $self->logcroak("Failed to write record [", $snp->str,
                           "] to '$file_name': ", $csv->error_diag);
      ++$records_written;
    }
  }

  close $out;

  return $records_written;
}

sub _build_snps {
  my ($self) = @_;

  # Default is to be empty
  my $records = [];

  if ($self->data_object) {
    my $content = $self->data_object->slurp;
    open my $fh, '<', \$content
      or $self->logconfess("Failed to open content string for reading: $!");
    $records = $self->_parse_snps($fh);
    close $fh or $self->logwarn("Failed to close a string handle");
  }
  elsif ($self->file_name && -e $self->file_name) {
    open my $fh, '<:encoding(utf8)', $self->file_name or
      $self->$self->logconfess("Failed to open file '", $self->file_name,
                               "' for reading: $!");
    $records = $self->_parse_snps($fh);
    close $fh;
  }

  return $records;
}

sub _build_references {
  my ($self) = @_;

  my @references;
  if ($self->data_object) {
    my @reference_name_avus = $self->data_object->find_in_metadata
      ($self->reference_genome_name_attr);

    foreach my $avu (@reference_name_avus) {
      push @references, WTSI::NPG::Genotyping::Reference->new
        (name => $avu->{value});
    }
  }

  return \@references;
}

sub _parse_snps {
  my ($self, $fh) = @_;

  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});

  my @snps;

  my $header = $csv->getline($fh);

  if ($header and @$header) {
    unless ($header->[0] =~ m{SNP_NAME}msx) {
      $self->logconfess("SNPSet data file '", $self->str,
                        "' is missing its header");
    }

    # Remove comment character from header
    $header->[0] =~ s/^\#//msx;

    $self->_write_column_names($header);

    my %gender_markers;

    while (my $record = $csv->getline($fh)) {
      $csv->combine(@$record);

      my $str = $csv->string;
      chomp $str;
      $self->debug("Building a new SNP set record from '$str'");

      my $num_fields = scalar @$record;
      unless ($num_fields == 6) {
        $self->logconfess("Invalid SNP set record '$str': ",
                          "expected 6 fields but found $num_fields");
      }

      my $snp = WTSI::NPG::Genotyping::SNP->new
        (name       => $record->[0],
         ref_allele => $record->[1],
         alt_allele => $record->[2],
         chromosome => $record->[3],
         position   => $record->[4],
         strand     => $record->[5],
         str        => $csv->string,
         snpset     => $self);

      my $key = $snp->name;
      if ($snp->is_gender_marker) {
        $self->debug("SNP ", $snp->name, " is a gender marker");

        if (exists $gender_markers{$key}) {
          my $x_marker;
          my $y_marker;

          # Remove from working hash on finding a pair
          if (is_HsapiensX($gender_markers{$key}->chromosome)) {
            $x_marker = delete $gender_markers{$key};
          }
          elsif (is_HsapiensY($gender_markers{$key}->chromosome)) {
            $y_marker = delete $gender_markers{$key};
          }

          if (is_HsapiensX($snp->chromosome)) {
            $x_marker = $snp;
          }
          elsif (is_HsapiensY($snp->chromosome)) {
            $y_marker = $snp;
          }

          push @snps, WTSI::NPG::Genotyping::GenderMarker->new
            (name     => $snp->name,
             x_marker => $x_marker,
             y_marker => $y_marker);
        }
        else {
          $gender_markers{$key} = $snp;
        }
      }
      else {
        $self->debug("SNP ", $snp->name, " is not a gender marker");
        push @snps, $snp;
      }
    }

    # Any unpaired markers are orphans; they must always appear in
    # pairs
    if (%gender_markers) {
      $self->logconfess("Orphan gender marker records for [",
                        join(', ', sort keys %gender_markers), "] in ",
                        $self->str);
    }
  }
  else {
    $self->debug("SNPSet data file '", $self->str, "' is empty");
  }

  $csv->eof or
    $self->logconfess("Parse error within '", $self->str, "': ",
                      $csv->error_diag);

  return \@snps;
}

sub _write_snp_record {
  my ($csv, $fh, $snp) = @_;

  return $csv->print($fh, [$snp->name, $snp->ref_allele, $snp->alt_allele,
                           $snp->chromosome, $snp->position, $snp->strand]);
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::SNPSet - Information on a set of SNPs used
in genotyping analyses

=head1 SYNOPSIS

  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "/seq/fluidigm/multiplex/qc.csv");

  my $plex = WTSI::NPG::Genotyping::SNPSet->new
    (data_object => $data_object);

=head1 DESCRIPTION

A wrapper for the CSV data files used to contain sets of SNP
information. It performs an eager parse of the file and provides
methods to access and manipulate the data.

If backed by an iRODS data object, instances expect to read data and
possibly metadata from that object. Writing data back to iRODS is not
supported. However, writing data back to regular files is supported,
so one may create a SNPSet, passing SNPs and the name of a
non-existent file to the constructor and then write, at which point
a new file will be created.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
