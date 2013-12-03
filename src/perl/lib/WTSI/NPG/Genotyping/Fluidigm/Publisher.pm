
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::Publisher;

use File::Basename qw(basename);
use File::Spec;
use File::Temp qw(tempdir);
use Moose;
use URI;

use WTSI::NPG::Genotyping::Fluidigm::ExportFile;
use WTSI::NPG::Genotyping::Metadata qw($FLUIDIGM_PLATE_NAME_META_KEY);
use WTSI::NPG::iRODS;
use WTSI::NPG::SimplePublisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'audience_uri' =>
  (is       => 'ro',
   isa      => 'URI',
   required => 1,
   default  => sub {
     my $uri = URI->new('http:');
     $uri->host('psd-production.internal.sanger.ac.uk');
     $uri->port(6600);

     return $uri;
   });

has 'publication_time' =>
  (is       => 'ro',
   isa      => 'DateTime',
   required => 1);

has 'resultset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::Fluidigm::ResultSet',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

=head2 publish

  Arg [1]    : Str iRODS path that will be the destination for publication
  Arg [2]    : Subset of plate addresses to publish. Optional, defaults to all.

  Example    : $export->publish('/foo', 'S01', 'S02')
  Description: Publish a ResultSet to an iRODS path.
  Returntype : Int number of addresses published

=cut

sub publish {
  my ($self, $publish_dest, @addresses) = @_;

  my $fluidigm_collection = $self->publish_directory($publish_dest);
  my $num_published = $self->publish_samples($fluidigm_collection, @addresses);

  return $num_published;
}

=head2 publish_samples

  Arg [1]    : Str iRODS path that will be the destination for publication
  Arg [2]    : Subset of plate addresses to publish. Optional, defaults to all.

  Example    : $export->publish_samples('/foo', 'S01', 'S02')
  Description: Publish the individual samples within a Fluidigm::ResultSet to an
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

  my $export_file = WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
    (file_name => $self->resultset->export_file);

  unless (@addresses) {
    @addresses = @{$export_file->addresses};
  }

  my $publisher = WTSI::NPG::SimplePublisher->new
    (irods         => $self->irods,
     accountee_uid => $self->accountee_uid,
     logger        => $self->logger);

  $self->debug("Publishing raw Fluidigm CSV data file '",
               $self->resultset->export_file, "'");
  my @meta =
    ([$FLUIDIGM_PLATE_NAME_META_KEY => $export_file->fluidigm_barcode],
     ['dcterms:audience'            => $self->audience_uri->as_string]);

  $publisher->publish_file($self->resultset->export_file, \@meta,
                           $publish_dest,
                           $self->publication_time);

  my $total = scalar @addresses;
  my $possible = $export_file->size;
  $self->debug("Publishing $total Fluidigm CSV data files ",
               "from a possible $possible");

  foreach my $address (@addresses) {
    eval {
      my $file = sprintf("%s/%s_%s.csv", $tmpdir, $address,
                         $export_file->fluidigm_barcode);
      $current_file = $file;

      my $record_count = $export_file->write_sample_assays($address, $file);
      $self->debug("Wrote $record_count records into '$file'");

      my @fingerprint = $export_file->fluidigm_fingerprint($address);
      my $data_object = $publisher->publish_file($file, \@fingerprint,
                                                 $publish_dest,
                                                 $self->publication_time);
      ++$num_published;
    };

    if ($@) {
      $self->error("Failed to publish '$current_file' to ",
                   "'$publish_dest': ", $@);
    }
    else {
      $self->debug("Published '$current_file': $num_published of $total ",
                   "from a possible $possible");
    }
  }

  return $num_published;
}

=head2 publish_directory

  Arg [1]    : Str iRODS path that will be the destination for publication

  Example    : $export->publish_directory('/foo')
  Description: Publish the directory with a Fluidigm::ResultSet to an
               iRODS path. Inserts a hashed directory path.
  Returntype : Str the newly created iRODS collection

=cut

sub publish_directory {
  my ($self, $publish_dest) = @_;

  my $irods = $self->irods;
  my $export_file = $self->resultset->export_file;
  my $md5 = $irods->md5sum($export_file);
  my $hash_path = $irods->hash_path($export_file, $md5);
  $self->debug("Checksum of file '$export_file' is '$md5'");

  my $dest_collection = $publish_dest;
  $dest_collection = File::Spec->catdir($publish_dest, $hash_path);

  my $fluidigm_collection;
  if ($irods->list_collection($dest_collection)) {
    $self->info("Skipping publication of Fluidigm data collection ",
                "'$dest_collection': already exists");

    my $dir = basename($self->resultset->directory);
    $fluidigm_collection = File::Spec->catdir($dest_collection, $dir);
  }
  else {
    $self->info("Publishing new Fluidigm data collection '$dest_collection'");
    $irods->add_collection($dest_collection);
    $fluidigm_collection = $irods->put_collection($self->resultset->directory,
                                                  $dest_collection);
  }

  return $fluidigm_collection;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::Publisher - An iRODS data publisher
for Fluidigm results.

=head1 SYNOPSIS

  my $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
    (directory => $dir);

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (irods            => $irods_handle,
     creator_uri      => $creator_uri,
     publisher_uri    => $publisher_uri,
     publication_time => DateTime->now,
     resultset        => $resultset);

  # Publish all
  $publisher->publish($publish_dest);

  # Publish selected addresses
  $publisher->publish($publish_dest, 'S01', 'S02');

=head1 DESCRIPTION

This class provides methods for publishing a complete Fluidigm dataset
to iRODS, with relevant primary metadata.

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
