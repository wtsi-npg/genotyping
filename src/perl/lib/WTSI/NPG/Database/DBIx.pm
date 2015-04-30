
package WTSI::NPG::Database::DBIx;

use DBIx::Class::Schema;
use Moose::Role;
use Try::Tiny;

has 'schema' =>
  (is       => 'rw',
   isa      => 'DBIx::Class::Schema',
   required => 0);

with 'WTSI::DNAP::Utilities::Loggable';

requires 'connect';

before 'connect' => sub {
  my ($self) = @_;

  unless ($self->is_connected) {
    $self->info('Connecting to ', $self->data_source);
  }
};

after 'connect' => sub {
  my ($self) = @_;

  # Forces connection to be opened immediately.
  $self->schema->storage->ensure_connected;

  if ($self->is_connected) {
    $self->info('Connected to ', $self->data_source);
  }
  else {
    $self->logconfess('Failed to connect to ', $self->data_source);
  }
};

sub disconnect {
  my ($self) = @_;

  if ($self->is_connected) {
    $self->info('Disconnecting from ', $self->data_source);
    $self->schema->storage->disconnect;
  }
  else {
    $self->warn("Attempted to disconnect when not connected");
  }

  return $self;
}

sub is_connected {
  my ($self) = @_;

  if ($self->schema) {
    $self->debug("Checking schema storage to see if we are connected");
    return $self->schema->storage->connected;
  }
}

=head2 in_transaction

  Arg [1]    : Subroutine reference
  Arg [n]    : Subroutine arguments

  Example    : $db->in_transaction(sub { my $ds = shift;
                                         my @sm = @_;
                                         foreach (@elt) {
                                           $ds->my_method($_);
                                         }
                                        }, $dataset, @elts);
  Description: Executes a subroutine in the context of a transaction
               which will rollback on error.
  Returntype : As subroutine.

=cut

sub in_transaction {
  my ($self, $code, @args) = @_;

  my @result;

  try {
    if ($self->is_connected) {
      @result = $self->schema->txn_do($code, @args);
    }
    else {
      $self->logcroak("Attempted to use a closed connection")
    }
  } catch {
    if ($_ =~ /Rollback failed/) {
      $self->logconfess("$_. Rollback failed! ",
                        "WARNING: data may be inconsistent.");
    } else {
      $self->logconfess("$_. Rollback successful");
    }
  };

  return wantarray ? @result : $result[0];
}

1;
