use utf8;

package WTSI::NPG::Publisher;

use File::Spec;
use Moose;

use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::DataObject;

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1);

has 'disperse' =>
  (is       => 'ro',
   isa      => 'Bool',
   required => 1,
   default  => 1);

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable', 'WTSI::NPG::Annotator';

=head2 publish_file

  Arg [1]    : file name
  Arg [2]    : Sample metadata
  Arg [3]    : Publication path in iRODS
  Arg [4]    : DateTime object of publication

  Example    : my $data_obj = $publisher->publish_file($file, \@metadata,
                                                       '/my/file', $now);
  Description: Publish a file to iRODS with metadata. Republish any
               file that is already published, but whose checksum has
               changed. This method distributes files into paths based on
               the file checksum. It attempts to identify, using metadata,
               any existing files in iRODS that represent the same
               experimental result, possibly having different checksums.
               It uses data object basename and metadata to define identity.
  Returntype : path to new iRODS data object

=cut

sub publish_file {
  my ($self, $file, $sample_meta, $publish_dest, $time) = @_;

  my $irods = $self->irods;
  my ($volume, $directories, $filename) = File::Spec->splitpath($file);
  my $md5 = $self->irods->md5sum($file);
  $self->debug("Checksum of file '$file' to be published is '$md5'");

  my $dest_collection = $publish_dest;
  $dest_collection = File::Spec->canonpath($dest_collection);

  unless (File::Spec->file_name_is_absolute($dest_collection)) {
    $dest_collection = $irods->working_collection . '/' . $dest_collection;
  }

  if ($self->disperse) {
    my $hash_path = $irods->hash_path($file, $md5);
    $dest_collection = $dest_collection . '/' . $hash_path;
  }

  unless ($irods->list_collection($dest_collection)) {
    $irods->add_collection($dest_collection);
  }

  my $target = $dest_collection. '/' . $filename;
  my $zone = $irods->find_zone_name($target);

  my @meta = @$sample_meta;
  my $meta_str = '[' . join(', ', map { join ' => ', @$_ } @meta) . ']';

  my $target_obj = WTSI::NPG::iRODS::DataObject->new($irods, $target);

  # Find existing data from same experiment, if any
  my @matching = $irods->find_objects_by_meta("/$zone", @meta);
  # Find existing copies of this file
  my @existing = grep { my ($vol, $dirs, $name) = File::Spec->splitpath($_);
                        $name eq $filename } @matching;
  my $existing_obj;

  if (scalar @existing >1) {
    $self->logconfess("While publishing '$target' identified by ",
                      $target_obj->meta_str,
                      " found >1 existing sample data: [",
                      join(', ', @existing), "]");
  }

  if (@existing) {
    $existing_obj = WTSI::NPG::iRODS::DataObject->new($irods, $existing[0]);
    $self->warn("While publishing '", $target_obj->str, "' identified by ",
                $meta_str, " found existing sample data: '",
                $existing_obj->str, "' identified by ",
                $existing_obj->meta_str);

    unless ($existing_obj->find_in_metadata('md5')) {
      $self->warn("Checksum metadata for existing sample data '",
                  $existing_obj->str, "' is missing");
      $self->info("Adding missing checksum metadata for existing sample data '",
                  $existing_obj->str, "'");

      my $existing_md5 = $existing_obj->calculate_checksum;
      $existing_obj->add_avu('md5', $existing_md5);
    }

    unless ($existing_obj->validate_checksum_metadata) {
      $self->error("Checksum metadata for existing sample data '",
                   $existing_obj->str, "' is out of date");
    }
  }

  # Even with different MD5 values, the new and old data objects could
  # have the same path because we use only the first 3 bytes of the
  # hash string to define the collection path and the file basenames
  # are identical.

  if ($target_obj->is_present) {
    my $target_md5 = $target_obj->calculate_checksum;
    $self->debug("Checksum of existing target '", $target_obj->str,
                 "' is '$target_md5'");
    if ($md5 eq $target_md5) {
      $self->info("Skipping publication of '", $target_obj->str,
                  "' because the checksum is unchanged");
    }
    else {
      $self->info("Republishing '", $target_obj->str, "' in situ ",
                  "because the checksum is changed");
      $irods->replace_object($file, $target_obj->str);

      foreach my $avu ($target_obj->find_in_metadata('md5')) {
        $target_obj->remove_avu($avu->{attribute}, $avu->{value});
      }

      push(@meta, $self->make_md5_metadata($file));
      push(@meta, $self->make_modification_metadata($time));
    }
  }
  elsif (@existing) {
    my $existing_md5 = $existing_obj->calculate_checksum;
    $self->debug("Checksum of existing object '", $existing_obj->str,
                 "' is '$existing_md5'");
    if ($md5 eq $existing_md5) {
      $self->info("Skipping publication of '", $target_obj->str,
                  "' because existing object '", $existing_obj->str,
                  "' exists with the same checksum");
    }
    else {
      $self->info("Moving '", $existing_obj->str, ' to ',
                  $target_obj->str, "' and republishing over it ",
                  "because the checksum is changed from ",
                  "'$existing_md5' to '$md5'");
      # The following moves any metadata too
      $irods->move_object($existing_obj->str, $target_obj->str);

      foreach my $avu ($target_obj->find_in_metadata('md5')) {
        $target_obj->remove_avu($avu->{attribute}, $avu->{value});
      }

      $irods->replace_object($file, $target_obj->str);

      push(@meta, $self->make_md5_metadata($file));
      push(@meta, $self->make_modification_metadata($time));
    }
  }
  else {
    $self->info("Publishing new object '$target'");
    $irods->add_object($file, $target_obj->str);

    my $creator_uri = $self->affiliation_uri;
    my $publisher_uri = $self->accountee_uri;
    push(@meta, $self->make_creation_metadata($creator_uri, $time,
                                              $publisher_uri));
    push(@meta, $self->make_md5_metadata($file));
  }

  push(@meta, $self->make_type_metadata($file));

  foreach my $m (@meta) {
    my ($attribute, $value, $units) = @$m;
    $target_obj->add_avu($attribute, $value, $units);
  }

  return $target;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Publisher - Basic file publishing to iRODS with metadata
and checksum tests.

=head1 SYNOPSIS

  my $publisher = WTSI::NPG::Publisher->new;
  my $data_obj = $publisher->publish_file($file, \@metadata,
                                          '/my/file', $now);

=head1 DESCRIPTION

This class provides general purpose file publishing functionality.

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
