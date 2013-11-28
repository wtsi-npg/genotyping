
use utf8;

package WTSI::NPG::iRODS::Collection;

use JSON;
use File::Spec;
use Moose;

use WTSI::NPG::iRODS;

with 'WTSI::NPG::iRODS::Path';

# Lazily load metadata from iRODS
around 'metadata' => sub {
  my ($orig, $self) = @_;

  unless ($self->has_metadata) {
    my @meta = $self->irods->get_collection_meta($self->str);
    $self->$orig(\@meta);
  }

  return $self->$orig;
};

sub is_present {
  my ($self) = @_;

  return $self->irods->list_collection($self->str);
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

    $absolute = File::Spec->catdir($self->irods->working_collection,
                                   $self->collection);
  }

  return WTSI::NPG::iRODS::Collection->new($self->irods, $absolute);
}

=head2 add_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->add_avu('foo', 'bar')
  Description: Add an AVU to an iRODS path (data object or collection)
               Return self.
  Returntype : WTSI::NPG::iRODS::Collection
  Caller     : general

=cut

sub add_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    my $units_str = defined $units ? "'$units'" : 'undef';
    $self->debug("Failed to add AVU {'$attribute', '$value', $units_str} ",
                 "to '", $self->str, "': AVU is already present");
  }
  else {
    $self->irods->add_collection_avu($self->str, $attribute, $value, $units);
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
  Returntype : WTSI::NPG::iRODS::Collection
  Caller     : general

=cut

sub remove_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    $self->irods->remove_collection_avu($self->str, $attribute, $value, $units);
  }
  else {
    my $units_str = defined $units ? "'$units'" : 'undef';
    $self->logcarp("Failed to remove AVU {'$attribute', '$value', $units_str} ",
                   "from '", $self->str, "': AVU is not present");
  }

  $self->clear_metadata;

  return $self;
}

sub grant_group_access {
  my ($self, $permission, @groups) = @_;

  my $path = $self->str;
  foreach my $group (@groups) {
    $self->info("Giving group '$group' -r '$permission' access to '$path'");
    $self->irods->set_group_access('-r', $permission, $group, $path);
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

  return $self->collection;
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

  my $spec = {collection => $self->collection,
              avus       => $self->metadata};

  return JSON->new->utf8->encode($spec);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
