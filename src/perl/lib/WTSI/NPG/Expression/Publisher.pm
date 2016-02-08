
use utf8;

package WTSI::NPG::Expression::Publisher;

use File::Spec;
use Moose;
use Try::Tiny;

use WTSI::NPG::Expression::InfiniumDataObject;
use WTSI::NPG::Expression::ResultSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::Publisher;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Accountable',
  'WTSI::NPG::Annotator', 'WTSI::NPG::Expression::Annotator';

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

has 'manifest' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Expression::ChipLoadingManifest',
   required => 1);

has 'data_files' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   required => 1);

has 'resultsets' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Expression::ResultSet]',
   required => 1,
   lazy     => 1,
   builder  => '_build_resultsets');

has 'sequencescape_db' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Database::Warehouse',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

sub publish {
  my ($self, $publish_dest) = @_;

  return $self->publish_samples($publish_dest);
}

sub publish_samples {
  my ($self, $publish_dest) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);

  my $num_published = 0;
  my $total = scalar @{$self->resultsets} * 2;
  $self->debug("Publishing $total Infinium files from a possible $total");

  foreach my $resultset (@{$self->resultsets}) {
    $num_published += $self->_publish_files($resultset, $publish_dest);
  }

  $self->info("Published $num_published/$total Infinium files ",
              "to '$publish_dest'");

  return $num_published;
}

sub _publish_files {
  my ($self, $resultset, $publish_dest) = @_;

  my $num_published = 0;
  my @meta;
  push @meta, $self->make_infinium_metadata($resultset);
  my @fingerprint = $self->infinium_fingerprint(@meta);

  # At the time of publication, check that the sample ID in the
  # manifest being used to publish the data corresponds to the one in
  # the warehouse. Later they may differ for short periods if the one
  # in the warehouse is changed (e.g. to correct a tracking error).

  my $plate     = $resultset->plate_id;
  my $well      = $resultset->well_id;
  my $sample_id = $resultset->sample_id;

  my $ssdb = $self->sequencescape_db;
  my $ss_sample = $ssdb->find_infinium_gex_sample($plate, $well);
  my $expected_sanger_id = $ss_sample->{sanger_sample_id};

  unless ($expected_sanger_id) {
    $self->error("Sample in plate '$plate' well '$well' ",
                 "with sample ID '$sample_id' was not found in the ",
                 "warehouse.");
  }

  unless ($sample_id eq $expected_sanger_id) {
    $self->error("Sample in plate '$plate' well '$well' ",
                 "has an incorrect Sanger sample ID '$sample_id' ",
                 "(expected '$expected_sanger_id'");
  }

  if ($expected_sanger_id && ($sample_id eq $expected_sanger_id)) {
    foreach my $file ($resultset->idat_file, $resultset->xml_file) {
      try {
        $self->_publish_file($file, $resultset, \@fingerprint, $publish_dest);
        ++$num_published;
        $self->info("Published '$file' to '$publish_dest'");
      } catch {
        $self->error("Failed to publish '$file' to '$publish_dest': ", $_);
      };
    }
  }

  return $num_published;
}

sub _publish_file {
  my ($self, $filename, $resultset, $fingerprint, $publish_dest) = @_;

  my $publisher =
    WTSI::NPG::Publisher->new(irods         => $self->irods,
                              accountee_uid => $self->accountee_uid,
                              logger        => $self->logger);

  my $obj_path = $publisher->publish_file($filename,
                                          $fingerprint,
                                          $publish_dest,
                                          $self->publication_time);

  my $obj = WTSI::NPG::Expression::InfiniumDataObject->new($self->irods,
                                                           $obj_path);
  $obj->update_secondary_metadata($self->sequencescape_db);

  return $obj;
}

sub _build_resultsets {
  my ($self) = @_;

  my $filesets = $self->_build_filesets;
  my @resultsets;

 SAMPLE:
  foreach my $sample (@{$self->manifest->samples}) {
    my $beadchip = $sample->{beadchip};
    my $section = $sample->{beadchip_section};

    unless ($filesets->{$beadchip}{$section}) {
       $self->warn("Failed to collate a resultset for beadchip ",
                   "'$beadchip' section '$section' because it did not ",
                   "contain any files");
       next SAMPLE;
    }

    my @fileset = @{$filesets->{$beadchip}{$section}};
    unless (scalar @fileset == 2) {
      $self->warn("Failed to collate a resultset for beadchip ",
                  "'$beadchip' section '$section' because it did not ",
                  "contain exactly 2 files: [",
                  join(", ", sort @fileset), "]");
      next SAMPLE;
    }

    my ($idat, $xml) = ('', '');

    foreach my $path (@fileset) {
      if    ($path =~ m{[.]idat$}msxi) { $idat = $path }
      elsif ($path =~ m{[.]xml$}msxi)  { $xml  = $path }
      else {
        $self->warn("Failed to collate a resultset for beadchip ",
                    "'$beadchip' section '$section' because it ",
                    "contained an expected file '$path'");
        next SAMPLE;
      }
    }

    if ($idat && $xml) {
      $self->debug("Collating a new resultset for $beadchip $section");
      my %initargs = %$sample;
      $initargs{idat_file} = $idat;
      $initargs{xml_file} = $xml;

      push @resultsets, WTSI::NPG::Expression::ResultSet->new(%initargs);
    }
    else {
      $self->warn("Failed to collate a resultset for beadchip ",
                  "'$beadchip' section '$section' because its file set was ",
                  "incomplete: [idat: '$idat', XML: '$xml']");
    }
  }

  return \@resultsets;
}

sub _build_filesets {
  my ($self) = @_;

  # Each hash chain $filesets{beadchip}{section} points to an array
  # containing the names of the 2 files in the set
  my %filesets;

  foreach my $path (sort  @{$self->data_files}) {
    my ($volume, $dirs, $filename) = File::Spec->splitpath($path);

    $self->debug("Preparing to collate '$filename' into a resultset");

    my ($beadchip, $section, $suffix) =
      $filename =~ m{^
                     (\d{10,12})     # beadchip
                     _([[:upper:]])  # beadchip section
                     _Grn            # channel, always Grn
                     [.](\S+)        # suffix
                     $}msxi;

    unless ($beadchip && $section && $suffix) {
      $self->warn("Failed to parse expression results filename '$filename'; ",
                  "ignoring it");
      next;
    }

    unless (exists $filesets{$beadchip}{$section}) {
      $filesets{$beadchip}{$section} = [];
    }

    push @{$filesets{$beadchip}{$section}}, $path;
  }

  return \%filesets;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015, 2016 Genome Research Limited. All
Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
