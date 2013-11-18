
package WTSI::NPG::iRODS::DataObject;

use JSON;
use File::Spec;
use Moose;

use WTSI::NPG::iRODS2;

with 'WTSI::NPG::iRODS::Path';

has 'data_object' => (is => 'ro', isa => 'Str', required => 1,
                      default => '.', lazy => 1,
                      predicate => 'has_data_object');

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 2 && ref $args[0] eq 'WTSI::NPG::iRODS2') {
    my ($volume, $collection, $data_name) = File::Spec->splitpath($args[1]);

    return $class->$orig(irods => $args[0], collection => $collection,
                         data_object =>$data_name);
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

=head2 add_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->add_avu('foo', 'bar')
  Description: Add an AVU to an iRODS path (data object or collection)
               Return self.
  Returntype : WTSI::NPG::iRODS::DataObject
  Caller     : general

=cut

sub add_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    $self->debug("Failed to add AVU {'$attribute', '$value', '$units'} ",
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
  Caller     : general

=cut

sub str {
  my ($self) = @_;

  return File::Spec->join($self->collection, $self->data_object);
}

=head2 json

  Arg [1]    : None

  Example    : $path->str
  Description: Return a canonical JSON representation of this path,
               including any AVUs.
  Returntype : Str
  Caller     : general

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
