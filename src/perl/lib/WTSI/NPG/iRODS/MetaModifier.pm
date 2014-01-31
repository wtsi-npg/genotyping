
use utf8;

package WTSI::NPG::iRODS::MetaModifier;

use File::Spec;
use Moose;

extends 'WTSI::NPG::iRODS::Communicator';

our $META_ADD_OP = 'add';
our $META_REM_OP = 'rem';

has 'operation' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   lazy     => 1,
   default  => $META_ADD_OP);

has '+executable' => (default => 'json-metamod');

around [qw(modify_object_meta)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::MetaLister ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub modify_collection_meta {
  my ($self, $collection, $attribute, $value, $units) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/} or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $collection = File::Spec->canonpath($collection);

  my $spec = {collection => $collection,
              avus       => [{attribute => $attribute,
                              value     => $value}]};
  if ($units) {
    $spec->{avus}->[0]->{units} = $units;
  }

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $collection;
}

sub modify_object_meta {
  my ($self, $object, $attribute, $value, $units) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/} or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              data_object => $data_name,
              avus        => [{attribute => $attribute,
                               value     => $value}]};
  if ($units) {
    $spec->{avus}->[0]->{units} = $units;
  }

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $object;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::MetaModifier

=head1 DESCRIPTION

A client that modifies iRODS metadata.

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
