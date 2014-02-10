
use utf8;

package WTSI::NPG::Database;

use Carp;
use Config::IniFiles;
use DBI;
use Moose;

has 'name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'inifile' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'ini' =>
  (is       => 'rw',
   isa      => 'Config::IniFiles',
   required => 0);

has 'data_source' =>
  (is       => 'rw',
   isa      => 'Str',
   default  =>  sub {
     my ($self) = @_;
     return $self->ini->val($self->name, 'data_source');
   },
   required => 1,
   lazy     => 1);

has 'username' =>
  (is       => 'rw',
   isa      => 'Str',
   default  => sub {
     my ($self) = @_;
     return $self->ini->val($self->name, 'username');
   },
   required => 1,
   lazy     => 1);

has 'password' =>
  (is       => 'rw',
   isa      => 'Str',
   default  => sub {
     my ($self) = @_;
     return $self->ini->val($self->name, 'password');
   },
   required => 1,
   lazy     => 1);

has 'dbh' =>
  (is       => 'rw',
   isa      => 'Any',
   required => 0);

with 'WTSI::NPG::Loggable';


sub BUILD {
  my ($self) = @_;

  my $ini = Config::IniFiles->new(-file => $self->inifile);
  $self->ini($ini);
}

=head2 connect

  Arg [n]    : key => value

  Example    : my $dbh = $db->connect(AutoCommit => 0, RaiseError => 1)->dbh;
  Description: Connect to the configured database using DBI. Additional
               DBI connection arguments may be supplied as key => value
               pairs.
  Returntype : WTSI::NPG::Database

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
  Returntype : 

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

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Database

=head1 DESCRIPTION

Base class for other genotyping database classes, providing
configuration of database login credentials via .ini-style files of
the form:

 [database name]
 data_source=<perl DBI data source string>
 user_name=<user name>
 password=<password>

The password value may be omitted.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
