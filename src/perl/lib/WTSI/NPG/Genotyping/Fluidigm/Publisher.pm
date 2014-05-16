
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::Publisher;

use File::Basename qw(basename);
use File::Spec;
use File::Temp qw(tempdir);
use Set::Scalar;
use Moose;
use URI;

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::ExportFile;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::Publisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable', 'WTSI::NPG::Annotator',
  'WTSI::NPG::Genotyping::Annotator';

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

has 'reference_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   default  => sub {
     return 'Homo_sapiens (1000Genomes)'
   });

has 'reference_zone' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   default  => sub { return '/' },
   writer   => '_set_reference_zone');

has 'resultset' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::Fluidigm::ResultSet',
   required => 1);

has 'snpsets' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Genotyping::SNPSet]',
   required => 1,
   lazy     => 1,
   builder  => '_build_snpsets');

has 'ss_warehouse_db' =>
  (is       => 'ro',
   # isa      => 'WTSI::NPG::Database::Warehouse',
   isa      => 'Object',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);

  # Ensure that a zone used as a surrogate for an iRODS path has a
  # leading slash
  my $zone = $self->reference_zone;
  unless ($zone =~ m{^/}) {
    $self->_set_reference_zone('/' . $zone);
  }
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

  my $publisher = WTSI::NPG::Publisher->new
    (irods         => $self->irods,
     accountee_uid => $self->accountee_uid,
     logger        => $self->logger);

  $self->debug("Publishing raw Fluidigm CSV data file '",
               $self->resultset->export_file, "'");
  my @meta =
    ([$self->fluidigm_plate_name_attr => $export_file->fluidigm_barcode],
     [$self->dcterms_audience_attr    => $self->audience_uri->as_string]);

  $publisher->publish_file($self->resultset->export_file, \@meta,
                           $publish_dest,
                           $self->publication_time);

  my $total = scalar @addresses;
  my $possible = $export_file->size;
  $self->debug("Publishing $total Fluidigm CSV data files ",
               "from a possible $possible");

  my @snpsets = $self->snpsets;

  foreach my $address (@addresses) {
    eval {
      my $file = sprintf("%s/%s_%s.csv", $tmpdir, $address,
                         $export_file->fluidigm_barcode);
      $current_file = $file;

      my $record_count = $export_file->write_assay_result_data($address, $file);
      $self->debug("Wrote $record_count records into '$file'");

      my @fingerprint = $export_file->fluidigm_fingerprint($address);
      my $rods_path = $publisher->publish_file($file, \@fingerprint,
                                               $publish_dest,
                                               $self->publication_time);

      # Build from local file to avoid and iRODS round trip with iget
      my $resultset = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
        ($file);
      my $snpset = $self->_find_resultset_snpset($resultset);
      my $snpset_name = $self->_find_snpset_name($snpset);

      my $obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
        ($self->irods, $rods_path)->add_avu($self->fluidigm_plex_name_attr,
                                            $snpset_name);

      # Now that adding the secondary metadata is fast enough, we can
      # run it inline here, so that the data are available
      # immediately.
      $obj->update_secondary_metadata($self->ss_warehouse_db);

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

  my $export_file = $self->resultset->export_file;
  my $md5         = $self->irods->md5sum($export_file);
  my $hash_path   = $self->irods->hash_path($export_file, $md5);
  $self->debug("Checksum of file '$export_file' is '$md5'");

  my $dest_collection = $publish_dest;
  $dest_collection = File::Spec->catdir($publish_dest, $hash_path);

  my $fluidigm_collection;
  if ($self->irods->list_collection($dest_collection)) {
    $self->info("Skipping publication of Fluidigm data collection ",
                "'$dest_collection': already exists");

    my $dir = basename($self->resultset->directory);
    $fluidigm_collection = File::Spec->catdir($dest_collection, $dir);
  }
  else {
    $self->info("Publishing new Fluidigm data collection '$dest_collection'");
    $self->irods->add_collection($dest_collection);
    $fluidigm_collection = $self->irods->put_collection
      ($self->resultset->directory, $dest_collection);
  }

  return $fluidigm_collection;
}

sub _build_snpsets {
  my ($self) = @_;

  my @snpset_paths = $self->irods->find_objects_by_meta
    ($self->reference_zone,
     [$self->fluidigm_plex_name_attr    => '%', 'like'],
     [$self->reference_genome_name_attr => $self->reference_name]);

  my @snpsets;
  foreach my $rods_path (@snpset_paths) {
    my $data_object = WTSI::NPG::iRODS::DataObject->new
      ($self->irods, $rods_path);
    push @snpsets, WTSI::NPG::Genotyping::SNPSet->new($data_object);
  }

  unless (@snpsets) {
    $self->logconfess("Failed to find any Fluidigm SNP sets for reference '",
                      $self->reference_name, "' in iRODS");
  }

  return \@snpsets;
}

sub _find_resultset_snpset {
  my ($self, $resultset) = @_;

  my @result_snp_names = $resultset->snp_names;
  my $expected_num_snps = scalar @result_snp_names;

  $self->debug("Finding set of $expected_num_snps SNPs ",
               "used by assay results in '",
               $resultset->str, "'");

  my @matched;
  foreach my $snpset (@{$self->snpsets}) {
    my @names = $snpset->data_object->find_in_metadata
      ($self->fluidigm_plex_name_attr);

    my @snp_names = $snpset->snp_names;
    my $num_snps = scalar @snp_names;

    $self->debug("Trying SNP set '", $snpset->str, "' of $num_snps SNPs");

    my $expected_names = Set::Scalar->new(@snp_names);
    my $result_names = Set::Scalar->new(@result_snp_names);

    if ($result_names == $expected_names) {
      push @matched, $snpset;
    }
    else {
      $self->debug("Ignoring SNP set '", $snpset->str, "' because of SNP ",
                   "differences. SNPS expected: ", $expected_names-> size,
                   ", SNPS in result: ", $result_names->size,
                   ". SNP set minus result set: ",
                   $expected_names->difference($result_names),
                   ", result set minus SNP set: ",
                   $result_names->difference($expected_names));
    }
  }

  my $num_matched = scalar @matched;

  unless ($num_matched > 0) {
    $self->logconfess("Failed to find the set of $expected_num_snps SNPs ",
                      "used by assay results in '", $resultset->str, "'");
  }

  if ($num_matched > 1) {
    $self->logconfess("Found $num_matched sets of $expected_num_snps SNPs ",
                      "for assay results in '", $resultset->str, "'. ",
                      "Unable to determine which is correct.");
  }

  return shift @matched;
}

sub _find_snpset_name {
  my ($self, $snpset) = @_;

  my @snpset_names = map { $_->{value} }
    $snpset->data_object->find_in_metadata($self->fluidigm_plex_name_attr);
  my $num_names = scalar @snpset_names;

  $num_names > 0 or
    $self->logconfess("No SNP set names defined in the metadata of '",
                      $snpset->str, "'");
  $num_names == 1 or
    $self->logconfess("$num_names SNP set names defined in the metadata of '",
                      $snpset->str, "': [", join(', ', @snpset_names), "]");

  return shift @snpset_names;
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
