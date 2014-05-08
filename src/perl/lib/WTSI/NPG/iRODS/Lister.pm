
use utf8;

package WTSI::NPG::iRODS::Lister;

use File::Spec;
use Moose;

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'json-list');

 # iRODS error code for non-existence
our $ITEM_DOES_NOT_EXIST = -310000;

around [qw(list_collection list_object
           get_collection_acl get_object_acl)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::Lister ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub list_object {
  my ($self, $object) = @_;

  my $response = $self->_list_object($object);
  my $path;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    $path = $self->_to_path_str($response);
  }

  return $path;
}

sub list_collection {
  my ($self, $collection, $recur) = @_;

  my $obj_specs;
  my $coll_specs;

  if ($recur) {
    ($obj_specs, $coll_specs) = $self->_list_collection_recur($collection);
  }
  else {
    ($obj_specs, $coll_specs) = $self->_list_collection($collection);
  }

  my @paths;
  if ($obj_specs and $coll_specs) {
    my @data_objects = map { $self->_to_path_str($_) } @$obj_specs;
    my @collections  = map { $self->_to_path_str($_) } @$coll_specs;
    @paths = (\@data_objects, \@collections);
  }

  return @paths;
}

sub get_object_acl {
  my ($self, $object) = @_;

  my $response = $self->_list_object($object);
  my $acl;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    $acl = $self->_to_acl($response);
  }

  $self->debug("ACL of '$object' is ", $self->_to_acl_str($acl));

  return @$acl;
}

sub get_collection_acl {
  my ($self, $collection) = @_;

  my ($object_specs, $collection_specs) = $self->_list_collection($collection);

  my @acl;
  if ($collection_specs) {
    my $collection_spec = shift @$collection_specs;
    my $acl = $self->_to_acl($collection_spec);

    $self->debug("ACL of '$collection' is ", $self->_to_acl_str($acl));

    @acl = @$acl;
  }

  return @acl;
}

sub _list_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/} or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              data_object => $data_name};
  my $response = $self->communicate($spec);
  $self->validate_response($response);

  return $response;
}

sub _list_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/} or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection => $collection};
  my $response = $self->communicate($spec);
  my @paths;

  if (ref $response eq 'HASH') {
    if (exists $response->{error}) {
      if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
        # Continue to return empty list
      }
      else {
        $self->report_error($response);
      }
    }
  }
  else {
    my @object_specs;
    my @collection_specs;

    foreach my $path (@$response) {
      if (exists $path->{data_object}) {
        push @object_specs, $path;
      }
      else {
        push @collection_specs, $path;
      }
    }

    @paths = (\@object_specs, \@collection_specs);
  }

  return @paths;
}

# Return two arrays of path specs, given a collection path to recurse
sub _list_collection_recur {
  my ($self, $collection) = @_;

  $self->debug("Recursing into '$collection'");
  my ($obj_specs, $coll_specs) = $self->_list_collection($collection);

  my @coll_specs = @$coll_specs;
  my $this_coll = shift @coll_specs;

  my @all_obj_specs  = @$obj_specs;
  my @all_coll_specs = ($this_coll);

  foreach my $sub_coll (@coll_specs) {
    my $path = $self->_to_path_str($sub_coll);
    $self->debug("Recursing into sub-collection '$path'");

    my ($sub_obj_specs, $sub_coll_specs) = $self->_list_collection_recur($path);
    push @all_obj_specs,  @$sub_obj_specs;
    push @all_coll_specs, @$sub_coll_specs;
  }

  return (\@all_obj_specs, \@all_coll_specs);
}

sub _to_path_str {
  my ($self, $path_spec) = @_;

  defined $path_spec or
    $self->logconfess('A defined path_spec argument is required');

  ref $path_spec eq 'HASH' or
    $self->logconfess('A HashRef path_spec argument is required');

  exists $path_spec->{collection} or
    $self->logconfess('The path_spec argument did not have a "collection" key');

  my $path = $path_spec->{collection};
  if (exists $path_spec->{data_object}) {
    $path = $path . '/' . $path_spec->{data_object};
  }

  return $path;
}

sub _to_acl {
  my ($self, $path_spec) = @_;

  defined $path_spec or
    $self->logconfess('A defined path_spec argument is required');

  ref $path_spec eq 'HASH' or
    $self->logconfess('A HashRef path_spec argument is required');

  exists $path_spec->{access} or
    $self->logconfess('The path_spec argument did not have an "access" key');

  return $path_spec->{access};
}

sub _to_acl_str {
  my ($self, $acl) = @_;

  defined $acl or
    $self->logconfess('A defined acl argument is required');

  ref $acl eq 'ARRAY' or
    $self->logconfess('An ArrayRef acl argument is required');

  my $str = '[';

  my @strs;
  foreach my $elt (@$acl) {
    push @strs, sprintf("%s:%s", $elt->{owner}, $elt->{level});
  }

  return '[' . join(', ', @strs) . ']' ;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Lister

=head1 DESCRIPTION

A client that lists iRODS data objects and collections as JSON.

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
