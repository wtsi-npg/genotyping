
use utf8;

package WTSI::NPG::iRODS::DataObject;

use JSON;
use File::Spec;
use Moose;
use Set::Scalar;

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

=head2 is_present

  Arg [1]    : None

  Example    : $path->is_present && print $path->str
  Description: Return true if the data object file exists in iRODS.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub is_present {
  my ($self) = @_;

  return $self->irods->list_object($self->str);
}

=head2 absolute

  Arg [1]    : None

  Example    : $path->absolute
  Description: Return the absolute path of the data object.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

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

=head2 calculate_checksum

  Arg [1]    : None

  Example    : $path->calculate_checksum
  Description: Return the MD5 checksum of the data object.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub calculate_checksum {
  my ($self) = @_;

  return $self->irods->calculate_checksum($self->str);
}

=head2 validate_checksum_metadata

  Arg [1]    : None

  Example    : $obj->validate_checksum_metadata
  Description: Return true if the MD5 checksum in the metadata of the
               object is identical to the MD5 calculated by iRODS.
  Returntype : boolean

=cut

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
               Return self. Clear the metadata cache.
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
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::DataObject

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

=head2 supersede_avus

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->supersede_avus('foo', 'bar')
  Description: Replace an AVU from an iRODS path (data object or collection)
               while removing any existing AVUs having under the same
               attribute. Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub supersede_avus {
  my ($self, $attribute, $value, $units) = @_;

  defined $attribute or
    $self->logcroak("A defined attribute argument is required");
  defined $value or
    $self->logcroak("A defined value argument is required");

  $self->debug("Superseding all '$attribute' metadata on '", $self->str, "'");

  my @matching = $self->find_in_metadata($attribute);
  my $num_matching = scalar @matching;

  $self->debug("Found $num_matching '$attribute' AVUs to supersede");

  my $num_processed = 0;
  if ($num_matching > 0) {
    # There are some AVUs present for this attribute, so remove them,
    # except in the case where one happens to be the same as we are
    # trying to add (to avoid removing it and immediately adding it
    # back).
    foreach my $avu (@matching) {
      ++$num_processed;
      $self->debug("Attempting to supersede $num_processed of ",
                   "$num_matching AVUs");

      my $old_attribute = $avu->{attribute};
      my $old_value     = $avu->{value};
      my $old_units     = $avu->{units};

      if (defined $units && defined $old_units &&
          $old_attribute eq $attribute &&
          $old_value     eq $value &&
          $old_units     eq $units) {
        # Units were defined in both and everything matches
        $self->debug("Not superseding (leaving in place) AVU ",
                     "{'$old_attribute', '$old_value', '$old_units'} on '",
                     $self->str, "' [$num_processed / $num_matching]");
      }
      elsif (!defined $units && !defined $old_units &&
             $old_attribute eq $attribute &&
             $old_value     eq $value) {
        # Units were undefined in both and everything else matches
        $self->debug("Not superseding (leaving in place) AVU ",
                     "{'$old_attribute', '$old_value', ''} on '",
                     $self->str, "' [$num_processed / $num_matching]");
      }
      else {
        # There were some differences
        my $old_units_str = defined $old_units ? "'$old_units'" : 'undef';
        $self->debug("Superseding AVU (removing) ",
                     "{'$old_attribute', '$old_value', ",
                     "$old_units_str} on '", $self->str, "' ",
                     "[$num_processed / $num_matching]");

        $self->remove_avu($old_attribute, $old_value, $old_units);

        my $units_str = defined $units ? "'$units'" : 'undef';
        $self->debug("Superseding with AVU (now adding) ",
                     "{'$attribute', '$value', ",
                     "$units_str} on '", $self->str, "' ",
                     "[$num_processed / $num_matching]");

        if ($self->get_avu($attribute, $value, $units)) {
          $self->debug("The superseding AVU ",
                       "{'$attribute', '$value', $units_str} ",
                       "is already in place on '",
                       $self->str, "' [$num_processed / $num_matching]");
        }
        else {
          $self->debug("Superseding with AVU {'$attribute', '$value', ",
                       "$units_str} on '", $self->str, "' ",
                       "[$num_processed / $num_matching]");
          $self->add_avu($attribute, $value, $units);
        }
      }
    }
  }
  else {
    # There are no AVUs present for this attribute, so just add it
    my $units_str = defined $units ? "'$units'" : 'undef';
    $self->debug("Not superseding with AVU (none currently with this) ",
                 "attribute {'$attribute', '$value', $units_str} on '",
                 $self->str, "'");

    $self->add_avu($attribute, $value, $units);
  }

  return $self;
}

sub get_permissions {
  my ($self) = @_;

  my $path = $self->str;
  return $self->irods->get_object_permissions($path);
}

=head2 set_permissions

  Arg [1]    : permission Str, one of 'null', 'read', 'write' or 'own'
  Arg [2]    : Array of owners (users and /or groups).

  Example    : $obj->set_permissions('read', 'user1', 'group1')
  Description: Set access permissions on the object. Return self.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub set_permissions {
  my ($self, $permission, @owners) = @_;


  my $perm_str = defined $permission ? $permission : 'null';

  my $path = $self->str;
  foreach my $owner (@owners) {
    $self->info("Giving owner '$owner' '$perm_str' access to '$path'");
    $self->irods->set_object_permissions($perm_str, $owner, $path);
  }

  return $self;
}

sub get_groups {
  my ($self, $level) = @_;

  $self->irods->get_object_groups($self->str, $level);
}

sub update_group_permissions {
  my ($self) = @_;

  # Record the current group permissions
  my @groups_permissions = $self->get_groups('read');
  my @groups_annotated = $self->expected_groups;

  $self->debug("Permissions before: [", join(", ", @groups_permissions), "]");
  $self->debug("Updated annotations: [", join(", ", @groups_annotated), "]");

  my $perms = Set::Scalar->new(@groups_permissions);
  my $annot = Set::Scalar->new(@groups_annotated);
  my @to_remove = $perms->difference($annot)->members;
  my @to_add    = $annot->difference($perms)->members;

  $self->debug("Groups to remove: [", join(', ', @to_remove), "]");
  $self->set_permissions('null', @to_remove);
  $self->debug("Groups to add: [", join(', ', @to_add), "]");
  $self->set_permissions('read', @to_add);
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

sub slurp {
  my ($self) = @_;

  my $content = $self->irods->slurp_object($self->str);

  defined $content or
    $self->logconfess("Slurped content of '", $self->str, "' was undefined");

  return $content;
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
