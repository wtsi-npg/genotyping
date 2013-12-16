
use utf8;

package WTSI::NPG::iRODS::DataObject;

use JSON;
use File::Spec;
use Moose;

use WTSI::NPG::iRODS;

with 'WTSI::NPG::iRODS::Path';

has 'data_object' =>
  (is        => 'ro',
   isa       => 'Str',
   required  => 1,
   lazy      => 1,
   default   => '.',
   predicate => 'has_data_object');

# TODO: Add a check so that a DataObject cannot be built from a path
# that is in fact a collection.
around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 2 && ref $args[0] eq 'WTSI::NPG::iRODS') {
    my ($volume, $collection, $data_name) = File::Spec->splitpath($args[1]);
    $collection = File::Spec->canonpath($collection);
    $collection ||= '.';

    return $class->$orig(irods       => $args[0],
                         collection  => $collection,
                         data_object => $data_name);
  }
  else {
    return $class->$orig(@_);
  }
};

# Lazily load metadata from iRODS
around 'metadata' => sub {
  my ($orig, $self) = @_;

  unless ($self->has_metadata) {
    my @meta = $self->irods->get_object_meta($self->str);
    $self->$orig(\@meta);
  }

  return $self->$orig;
};

sub is_present {
  my ($self) = @_;

  return $self->irods->list_object($self->str);
}

sub absolute {
  my ($self) = @_;

  my $absolute;
  if (File::Spec->file_name_is_absolute($self->str)) {
    $absolute = $self->str;
  }
  else {
    unless ($self->irods) {
      $self->logconfess("Failed to make '", $self->str, "' into an absolute ",
                        "path because it has no iRODS handle attached.");
    }

    $absolute = File::Spec->catfile($self->irods->working_collection,
                                    $self->collection, $self->data_object);
  }

  return WTSI::NPG::iRODS::DataObject->new($self->irods, $absolute);
}

sub calculate_checksum {
  my ($self) = @_;

  return $self->irods->calculate_checksum($self->str);
}

sub validate_checksum_metadata {
  my ($self) = @_;

  return $self->irods->validate_checksum_metadata($self->str);
}

=head2 add_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->add_avu('foo', 'bar')
  Description: Add an AVU to an iRODS path (data object or collection)
               Return self.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub add_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    my $units_str = defined $units ? "'$units'" : 'undef';

    $self->debug("Failed to add AVU {'$attribute', '$value', $units_str} ",
                 "to '", $self->str, "': AVU is already present");
  }
  else {
    $self->irods->add_object_avu($self->str, $attribute, $value, $units);
  }

  $self->clear_metadata;

  return $self;
}

=head2 remove_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->remove_avu('foo', 'bar')
  Description: Remove an AVU from an iRODS path (data object or collection)
               Return self.
  Returntype : WTSI::NPG::iRODS::DataObject
  Caller     : general

=cut

sub remove_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    $self->irods->remove_object_avu($self->str, $attribute, $value, $units);
  }
  else {
    $self->logcarp("Failed to remove AVU {'$attribute', '$value', '$units'} ",
                   "from '", $self->str, "': AVU is not present");
  }

  $self->clear_metadata;

  return $self;
}

sub grant_group_access {
  my ($self,  $permission, @groups) = @_;

  my $path = $self->str;
  foreach my $group (@groups) {
    $self->info("Giving group '$group' '$permission' access to '$path'");
    $self->irods->set_group_access($permission, $group, $path);
  }
}

=head2 str

  Arg [1]    : None

  Example    : $path->str
  Description: Return an absolute path string in iRODS.
  Returntype : Str

=cut

sub str {
  my ($self) = @_;

  return File::Spec->join($self->collection, $self->data_object);
}

=head2 json

  Arg [1]    : None

  Example    : $path->json
  Description: Return a canonical JSON representation of this path,
               including any AVUs.
  Returntype : Str

=cut

sub json {
  my ($self) = @_;

  my $spec = {collection  => $self->collection,
              data_object => $self->data_object,
              avus        => $self->metadata};

  return JSON->new->utf8->encode($spec);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::DataObject - An iRODS data object.

=head1 DESCRIPTION

Represents a data object and provides methods for adding and removing
metdata, applying checksums and setting access permissions.

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
