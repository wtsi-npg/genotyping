use utf8;

package WTSI::NPG::SimplePublisher;

use File::Spec;
use Moose;

use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           has_consent
                           make_creation_metadata
                           make_md5_metadata
                           make_type_metadata
                           make_modification_metadata
                           make_sample_metadata);

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1);

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable';

=head2 publish_file

  Arg [1]    : File name
  Arg [2]    : Metadata
  Arg [3]    : Publication path in iRODS
  Arg [4]    : DateTime object of publication

  Example    : my $data_obj = $publisher->publish_file($file, \@metadata,
                                                       '/my/file', $now);
  Description: Publish a file to iRODS with attendant metadata. Republish any
               file that is already published, but whose checksum has
               changed. This method does not look for other instances of
               the same data that may already be in another location in iRODS.
               It uses absolute data object paths to determine identity.
  Returntype : path to new iRODS data object

=cut

sub publish_file {
  my ($self, $file, $metadata, $publish_dest, $time) = @_;

  my $irods = $self->irods;
  my ($volume, $directories, $filename) = File::Spec->splitpath($file);
  my $md5 = $self->irods->md5sum($file);
  $self->debug("Checksum of file '$file' to be published is '$md5'");

  my $dest_path = File::Spec->canonpath($publish_dest);
  my $dest = WTSI::NPG::iRODS::Collection->new($irods, $dest_path)->absolute;
  unless ($dest->is_present) {
    $irods->add_collection($dest->str);
  }

  my $target = $dest->str . '/' . $filename;
  my $zone = $irods->find_zone_name($target);
  my @meta = @$metadata;

  my $target_obj = WTSI::NPG::iRODS::DataObject->new($irods, $target);

  if ($target_obj->is_present) {
    my $target_md5 = $target_obj->calculate_checksum;
    $self->debug("Checksum of existing target '$target' is '$target_md5'");
    if ($md5 eq $target_md5) {
      $self->info("Skipping publication of '$target' because the checksum ",
                  "is unchanged");
    }
    else {
      $self->info("Republishing '$target' in situ because the checksum ",
                  "is changed");
      $target = $irods->replace_object($file, $target_obj->str);

      foreach my $avu ($target_obj->find_in_metadata('md5')) {
        $target_obj->remove_avu($avu->{attribute}, $avu->{value});
      }

      push(@meta, make_md5_metadata($file));
      push(@meta, make_modification_metadata($time));
    }
  }
  else {
    $self->info("Publishing new object '$target'");
    $target = $irods->add_object($file, $target_obj->str);

    my $creator_uri = $self->affiliation_uri;
    my $publisher_uri = $self->accountee_uri;
    push(@meta, make_md5_metadata($file));
    push(@meta, make_creation_metadata($creator_uri, $time, $publisher_uri));
  }

  push(@meta, make_type_metadata($file));

  foreach my $m (@meta) {
    my ($attribute, $value, $units) = @$m;
    $target_obj->add_avu($attribute, $value, $units);
  }

  return $target_obj->str;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;


__END__

=head1 NAME

WTSI::NPG::SimplePublisher - Basic file publishing to iRODS with
metadata and checksum tests.

=head1 SYNOPSIS

  my $publisher = WTSI::NPG::SimplePublisher->new;
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
