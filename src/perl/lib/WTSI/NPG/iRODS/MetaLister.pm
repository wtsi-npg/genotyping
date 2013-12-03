
use utf8;

package WTSI::NPG::iRODS::MetaLister;

use JSON;
use Moose;

with 'WTSI::NPG::Startable';

has '+executable' => (default => 'json-metalist');

around [qw(list_collection_meta list_object_meta)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::MetaLister ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub list_collection_meta {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/} or
    $self->logconfess("An absolute object path argument is required: ",
                      "recieved '$collection'");

  $collection = File::Spec->canonpath($collection);

  my $spec = {collection => $collection};
  my $json = JSON->new->utf8->encode($spec);

  return $self->_list_path_meta($json);
}

sub list_object_meta {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/} or
    $self->logconfess("An absolute object path argument is required: ",
                      "recieved '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              data_object => $data_name};
  my $json = JSON->new->utf8->encode($spec);

  return $self->_list_path_meta($json);
}

sub _list_path_meta {
  my ($self, $path_spec) = @_;

  defined $path_spec or
    $self->logconfess('A defined JSON path spec argument is required');

  my $parser = JSON->new->max_size(4096);
  my $result;

  ${$self->stdin} .= $path_spec;
  ${$self->stderr} = '';

  $self->debug("Sending JSON path spec $path_spec to ", $self->executable);

  while ($self->harness->pumpable && !defined $result) {
    $self->harness->pump;
    $result = $parser->incr_parse(${$self->stdout});
    ${$self->stdout} = '';
  }

  # TODO -- factor out JSON protocol handling into a Role
  if (exists $result->{error}) {
    $self->logconfess($result->{error}->{message});
  }

  exists $result->{avus} or
    $self->logconfess('The returned path spec did not have an "avus" key');

  return @{$result->{avus}};
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::MetaLister

=head1 DESCRIPTION

A client that lists iRODS metadata as JSON.

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
