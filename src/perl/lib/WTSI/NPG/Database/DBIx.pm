
package WTSI::NPG::Database::DBIx;

use DBIx::Class::Schema;
use Moose::Role;
use Try::Tiny;

our $VERSION = '';

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
    if ($_ =~ m{Rollback failed}msx) {
      $self->logconfess("$_. Rollback failed! ",
                        "WARNING: data may be inconsistent.");
    } else {
      $self->logconfess("$_. Rollback successful");
    }
  };

  return wantarray ? @result : $result[0];
}

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Database::DBIx

=head1 DESCRIPTION

A Moose role providing utility methods for databases using
DBIx::Class.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
