
use utf8;

package WTSI::NPG::iRODS::ACLModifier;

use File::Spec;
use JSON;
use Moose;

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'json-chmod');

around [qw(chmod_object chmod_collection)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::ACLModifier ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub chmod_object {
  my ($self, $permission, $owner, $object) = @_;

  defined $permission or
    $self->logconfess('A defined permission argument is required');
  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/} or
      $self->logconfess("An absolute object path argument is required: ",
                        "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              data_object => $data_name,
              access      => [{owner => $owner,
                               level => $permission}]};

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $object;
}

sub chmod_collection {
  my ($self, $level, $owner, $collection) = @_;

  defined $level or
    $self->logconfess('A defined level argument is required');
  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/} or
      $self->logconfess("An absolute collection path argument is required: ",
                        "received '$collection'");

  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              access      => [{owner => $owner,
                               level => $level}]};

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $collection;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::ACLModifier

=head1 DESCRIPTION

A client that modifies iRODS access control lists.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
