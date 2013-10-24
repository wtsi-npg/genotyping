
package WTSI::NPG::iRODS::Path;

use JSON;
use File::Spec;
use Moose;

use WTSI::NPG::iRODS qw(add_collection_meta
                        add_object_meta
                        get_collection_meta
                        get_object_meta
                        remove_collection_meta
                        remove_object_meta);

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Annotatable';

has 'collection' => (is => 'ro', isa => 'Str', required => 1,
                     predicate => 'has_collection');

has 'data_object' => (is => 'ro', isa => 'Str',
                      predicate => 'has_data_object');

has '+metadata' => (predicate => 'has_metadata',
                    clearer => 'clear_metadata');

# Permit the constructor to use an iRODS path as its sole argument
around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # TODO -- Improve this method so that we can pass an array of path
  # elements to the constructor. The elements will be joined using the
  # path separator to form a complete path.

  if (@args == 1 && !ref $args[0]) {
    # Ends with '/' therefore is a collection
    if ($args[0] eq '.' ||
        $args[0] =~ m{/$}) {
      return $class->$orig(collection => $args[0]);
    }
    else {
      my ($volumes, $directory, $filename) = File::Spec->splitpath($args[0]);
      return $class->$orig(collection => $directory,
                           data_object => $filename);
    }
  }
  else {
    return $class->$orig(@_);
  }
};

# Lazily load metadata from iRODS
around 'metadata' => sub {
  my ($orig, $self) = @_;

  unless ($self->has_metadata) {
    my %meta;
    if ($self->has_data_object) {
      %meta = get_object_meta($self->str);
    }
    else {
      %meta = get_collection_meta($self->str);
    }

    my @meta;
    foreach my $attr (sort keys %meta) {
      unless (ref $meta{$attr} eq 'ARRAY') {
        $self->logconfess("Malformed value for attribute '$attr': ",
                          "expected an ArrayRef");
      }

      unless (scalar @{$meta{$attr}} > 0) {
        $self->logconfess("Malformed value for attribute '$attr': ",
                          "value array was empty");
      }

      # TODO -- remove this when we support fully units in metadata
      foreach my $value (sort @{$meta{$attr}}) {
        push @meta, [$attr, $value, ""];
      }
    }
    $self->$orig(\@meta);
  }

  return $self->$orig;
};

=head2 add_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->add_avu('foo', 'bar')
  Description: Add an AVU to an iRODS path (data object or collection)
               Return self.
  Returntype : WTSI::NPG::iRODS::Path
  Caller     : general

=cut

sub add_avu {
  my ($self, $attribute, $value, $units) = @_;
  $units ||= '';

  my @meta = @{$self->metadata};
  my @exists = grep { $_->[0] eq $attribute &&
                      $_->[1] eq $value &&
                      $_->[2] eq $units } @meta;
  if (@exists) {
    $self->debug("Failed to add AVU ['$attribute' '$value' '$units'] ",
                 "to '", $self->str, "': AVU is already present");
  }
  else {
    if ($self->has_data_object) {
      add_object_meta($self->str, $attribute, $value, $units);
    }
    else {
      add_collection_meta($self->str, $attribute, $value, $units);
    }
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
  Returntype : WTSI::NPG::iRODS::Path
  Caller     : general

=cut

sub remove_avu {
  my ($self, $attribute, $value, $units) = @_;
  $units ||= '';

  my @meta = @{$self->metadata};
  my @exists = grep { $_->[0] eq $attribute &&
                      $_->[1] eq $value &&
                      $_->[2] eq $units } @meta;

  if (@exists) {
    if ($self->has_data_object) {
      remove_object_meta($self->str, $attribute, $value, $units);
    }
    else {
      remove_collection_meta($self->str, $attribute, $value, $units);
    }
  }
  else {
    $self->logcarp("Failed to remove AVU ['$attribute' '$value' '$units'] ",
                   "from '", $self->str, "': AVU is not present");
  }

  $self->clear_metadata;

  return $self;
}

=head2 get_avu

  Arg [1]    : attribute
  Arg [2]    : value (optional)
  Arg [2]    : units (optional)

  Example    : $path->get_avu('foo')
  Description: Return a single matching AVU. If multiple candidate AVUs
               match the arguments, an error is raised.
  Returntype : Array
  Caller     : general

=cut

sub get_avu {
  my ($self, $attribute, $value, $units) = @_;
  $attribute or $self->logcroak("An attribute argument is required");

  my @meta = @{$self->metadata};
  my @exists;

  if ($value && $units) {
    @exists = grep { $_->[0] eq $attribute &&
                     $_->[1] eq $value &&
                     $_->[2] eq $units } @meta;
  }
  elsif ($value) {
    @exists = grep { $_->[0] eq $attribute &&
                     $_->[1] eq $value } @meta;
  }
  else {
    @exists = grep { $_->[0] eq $attribute } @meta;
  }

  if (scalar @exists > 1) {
    $value ||= '';
    $units ||= '';

    my $fn = sub { sprintf("['%s' -> '%s', '%s']",
                           $_[0]->[0], $_[0]->[1], $_[0]->[2]) };
    my $matched = join ", ", map { $fn->($_) } @exists;

    $self->logconfess("Failed to get a single AVU matching ",
                      "['$attribute', '$value', '$units']:",
                      " matched [$matched]");
  }

  my $avu = $exists[0];

  return @$avu;
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

  my @avus = map { { attribute => $_->[0],
                     value     => $_->[1],
                     units     => $_->[2] } } @{$self->metadata};

  my $json = {collection => $self->collection,
              avus       => \@avus};
  if ($self->has_data_object) {
    $json->{data_object} = $self->data_object;
  }

  return to_json($json);
}

sub _sort_metadata {
  my ($self) = @_;

  sort { $a->[0] cmp $b->[0] ||
         $a->[1] cmp $b->[1] ||
         $a->[2] cmp $b->[2] } @{$self->metadata};
}


no Moose;

1;
