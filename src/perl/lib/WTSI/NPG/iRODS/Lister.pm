
use utf8;

package WTSI::NPG::iRODS::Lister;

use File::Spec;
use JSON;
use Moose;

with 'WTSI::NPG::Startable';

has '+executable' => (default => 'json-list');

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
                      "recieved '$object'");

  my ($volume, $coll_name, $data_name) = File::Spec->splitpath($object);
  $coll_name = File::Spec->canonpath($coll_name);

  # TODO -- factor out JSON protocol handling into a Role
  my $json = qq({"collection": "$coll_name", "data_object": "$data_name"});
  my $parser = JSON->new->max_size(4096);
  my $result;

  ${$self->stdin} .= $json;
  ${$self->stderr} = '';

  while ($self->harness->pumpable && !defined $result) {
    $self->harness->pump;
    $result = $parser->incr_parse(${$self->stdout});
    ${$self->stdout} = '';
  }

  # TODO -- factor out JSON protocol handling into a Role
  if (exists $result->{error}) {
    $self->logconfess($result->error->{message});
  }

  return $self->_to_path_str($result);
}

sub list_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/} or
    $self->logconfess("An absolute collection path argument is required: ",
                      "recieved '$collection'");

  # TODO -- factor out JSON protocol handling into a Role
  my $path_spec = qq({"collection": "$collection"});
  my $parser = JSON->new->max_size(1024 *1024);
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
  if (ref $result eq 'HASH') {
    if (exists $result->{error}) {
      $self->logconfess($result->{error}->{message});
    }
  }

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

  return (\@data_objects, \@collections);
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
