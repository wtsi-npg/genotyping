
use utf8;

package WTSI::NPG::iRODS::Lister;

use File::Spec;
use JSON;
use Moose;

with 'WTSI::NPG::Startable';

has '+executable' => (default => 'json-list');

 # iRODS error code for non-existance
our $ITEM_DOES_NOT_EXIST = -310000;

around [qw(list_collection list_object)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::Lister ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub list_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/} or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  # TODO -- factor out JSON protocol handling into a Role
  my $spec = {collection  => $collection,
              data_object => $data_name};
  my $json = JSON->new->utf8->encode($spec);
  my $parser = JSON->new->utf8->max_size(4096);
  my $result;

  ${$self->stdin} .= $json;
  ${$self->stderr} = '';

  eval {
    # baton send JSON responses on a single line
    $self->harness->pump until ${$self->stdout} =~ m{[\r\n]$};
    $result = $parser->decode(${$self->stdout});
    ${$self->stdout} = '';
  };

  if ($@) {
    $self->error("JSON parse error on: '", ${$self->stdout}, "': ", $@);
  }

  unless (defined $result) {
    $self->logconfess("Failed to get a result from '$json'");
  }

  unless (ref $result eq 'HASH' ) {
    $self->logconfess("Failed to get a hash result from '$json'; got ",
                      ref $result);
  }

  my $path;
  # TODO -- factor out JSON protocol handling into a Role
  if (exists $result->{error}) {
    if ($result->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->logconfess($result->{error}->{message}, " Error code: ",
                        $result->{error}->{code});
    }
  }
  else {
    $path = $self->_to_path_str($result);
  }

  return $path;
}

sub list_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/} or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");

  # TODO -- factor out JSON protocol handling into a Role
  my $spec = {collection => $collection};
  my $json = JSON->new->utf8->encode($spec);
  my $parser = JSON->new->utf8->max_size(1024 *1024);
  my $result;

  ${$self->stdin} .= $json;
  ${$self->stderr} = '';

  $self->debug("Sending JSON spec $json to ", $self->executable);

  eval {
    # baton send JSON responses on a single line
    $self->harness->pump until ${$self->stdout} =~ m{[\r\n]$};
    $result = $parser->decode(${$self->stdout});
    ${$self->stdout} = '';
  };

  if ($@) {
    $self->error("JSON parse error on: '", ${$self->stdout}, "': ", $@);
  }

  my @paths;

  # TODO -- factor out JSON protocol handling into a Role
  if (ref $result eq 'HASH') {
    if (exists $result->{error}) {
      if ($result->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
        # Continue to return empty list
      }
      else {
        $self->logconfess($result->{error}->{message}, " Error code: ",
                          $result->{error}->{code});
      }
    }
  }
  else {
    my @data_objects;
    my @collections;

    foreach my $path_spec (@$result) {
      my $path = $self->_to_path_str($path_spec);

      if (exists $path_spec->{data_object}) {
        push @data_objects, $path;
      }
      else {
        push @collections, $path;
      }
    }

    @paths =  (\@data_objects, \@collections);
  }

  return @paths;
}

# TODO -- factor out JSON protocol handling into a Role
sub _to_path_str {
  my ($self, $path_spec) = @_;

  defined $path_spec or
    $self->logconfess('A defined path_spec argument is required');

  ref $path_spec eq 'HASH' or
    $self->logconfess('A defined path_spec argument is required');

  exists $path_spec->{collection} or
    $self->logconfess('The path_spec argument did not have a "collection" key');

  my $path = $path_spec->{collection};
  if (exists $path_spec->{data_object}) {
    $path = $path . '/' . $path_spec->{data_object};
  }

  return $path;
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
