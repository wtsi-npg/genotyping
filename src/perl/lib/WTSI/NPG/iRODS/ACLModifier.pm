
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
