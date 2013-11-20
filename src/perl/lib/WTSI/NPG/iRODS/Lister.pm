
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
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  # TODO -- factor out JSON protocol handling into a Role
  my $spec = {collection  => $collection,
              data_object => $data_name};
  my $json = JSON->new->utf8->encode($spec);

  my $parser = JSON->new->max_size(4096);
  my $result;

  ${$self->stdin} .= $json;
  ${$self->stderr} = '';

  while ($self->harness->pumpable && !defined $result) {
    $self->harness->pump;
    $result = $parser->incr_parse(${$self->stdout});
    ${$self->stdout} = '';
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
    # Code for non-existance
    if ($result->{error}->{code} == -310000) {
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
                      "recieved '$collection'");

  # TODO -- factor out JSON protocol handling into a Role
  my $spec = {collection => $collection};
  my $json = JSON->new->utf8->encode($spec);

  my $parser = JSON->new->max_size(1024 *1024);
  my $result;

  ${$self->stdin} .= $json;
  ${$self->stderr} = '';

  $self->debug("Sending JSON spec $json to ", $self->executable);

  while ($self->harness->pumpable && !defined $result) {
    $self->harness->pump;
    $result = $parser->incr_parse(${$self->stdout});
    ${$self->stdout} = '';
  }

  my @paths;

  # TODO -- factor out JSON protocol handling into a Role
  if (ref $result eq 'HASH') {
    if (exists $result->{error}) {
      # Code for non-existance
      if ($result->{error}->{code} == -310000) {
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
