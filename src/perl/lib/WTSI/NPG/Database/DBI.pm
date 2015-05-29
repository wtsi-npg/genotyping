
package WTSI::NPG::Database::DBI;

use DBI;
use Moose::Role;
use Try::Tiny;

has 'dbh' =>
  (is       => 'rw',
   isa      => 'Any',
   required => 0);

=head2 connect

  Arg [n]    : key => value

  Example    : my $dbh = $db->connect(AutoCommit => 0, RaiseError => 1)->dbh;
  Description: Connect to the configured database using DBI. Additional
               DBI connection arguments may be supplied as key => value
               pairs.
  Returntype : WTSI::NPG::Database::DBI

=cut

sub connect {
  my ($self, %args) = @_;

  unless ($self->dbh) {
    $self->info('Connecting to ', $self->data_source);
    $self->dbh(DBI->connect($self->data_source,
                            $self->username,
                            $self->password,
                            \%args));
  }

  return $self;
}

=head2 disconnect

  Arg [1]    : None

  Example    : $db->disconnect
  Description: Disconnect the database handle.
  Returntype : WTSI::NPG::Database::DBI

=cut

sub disconnect {
  my ($self) = @_;
  if ($self->is_connected) {
    $self->info('Disconnecting from ', $self->data_source);
    $self->dbh->disconnect;
  }
  else {
    $self->warn("Attempted to disconnect when not connected");
  }

  return $self;
}

=head2 is_connected

  Arg [1]    : None

  Example    : $db->is_connected
  Description: Return true if the database handle is connected.
  Returntype : boolean

=cut

sub is_connected {
  my ($self) = @_;

  return defined $self->dbh && $self->dbh->ping;
}

1;
