use utf8;

package WTSI::NPG::Database;

use strict;
use warnings;
use Carp;
use Config::IniFiles;
use DBI;
use Log::Log4perl;

=head2 new

  Arg [1]    : name => string
  Arg [2]    : inifile => string

  Example    : WTSI::NPG::Database::<some class>->new
                 (name => 'my_database', inifile => 'my_database.ini')
  Description: Return a new database handle configured from an
               .ini-style file.
  Returntype : WTSI::NPG::Database

=cut

sub new {
   my ($class, @args) = @_;

   my $self = {};
   bless($self, $class);
   $self->configure(@args);
   return $self;
}

=head2 configure

  Arg [1]    : name => string
  Arg [2]    : inifile => string

  Example    : $db->configure(name => 'my_database',
                              inifile => 'my_database.ini')
  Description: Configure an exisiting database handle from an
               .ini-style file.
  Returntype : WTSI::NPG::Database

=cut

sub configure {
  my ($self, %args) = @_;

  $self->name($args{name});
  $self->inifile($args{inifile});

  unless ($self->name) {
    $self->log->logconfess('The data source name was not defined.')
  }
  unless ($self->inifile) {
    $self->log->logconfess('The ini file was not defined.')
  }

  my $ini = Config::IniFiles->new(-file => $self->inifile);
  $self->data_source($ini->val($self->name, 'data_source'));
  $self->username($ini->val($self->name, 'username'));
  $self->password($ini->val($self->name, 'password'));

  my $log = $ini->val($self->name, 'log') || 'npg';
  $self->log(Log::Log4perl->get_logger($log));

  return $self;
}


=head2 inifile

  Arg [1]    : None

  Example    : $db->inifile
  Description: Return the current .ini-style file.
  Returntype : string

=cut

sub inifile {
  my ($self, @args) = @_;
  if (@args) {
    $self->{_inifile} = $args[0];
  }

  return $self->{_inifile};
}


=head2 connect

  Arg [n]    : key => value

  Example    : my $dbh = $db->connect(AutoCommit => 0, RaiseError => 1)->dbh;
  Description: Connect to the configured database using DBI. Additional
               DBI connection arguments may be supplied as key => value
               pairs.
  Returntype : WTSI::NPG::Database

=cut

## no critic

sub connect {
  my ($self, %args) = @_;

  unless ($self->{_dbh}) {
    $self->log->info('Connecting to ', $self->data_source);
    $self->{_dbh} = DBI->connect($self->data_source,
                                 $self->username,
                                 $self->password,
                                 \%args);
  }

  return $self;
}

## use critic

=head2 disconnect

  Arg [1]    : None

  Example    : $db->disconnect
  Description: Disconnect the database handle.
  Returntype : 

=cut

sub disconnect {
  my ($self) = @_;
  $self->log->info('Disconnecting from ', $self->data_source);
  $self->dbh->disconnect;

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


=head2 dbh

  Arg [1]    : None

  Example    : $db->dbh
  Description: Return the current database handle.
  Returntype : DBI handle

=cut

sub dbh {
  my ($self) = @_;
  return $self->{_dbh};
}


=head2 name

  Arg [1]    : None

  Example    : $db->name
  Description: Return the current database name.
  Returntype : string

=cut

sub name {
  my ($self, @args) = @_;
  if (@args) {
    $self->{_name} = $args[0];
  }

  return $self->{_name};
}


=head2 data_source

  Arg [1]    : None

  Example    : $db->data_source
  Description: Return the current database data source.
  Returntype : string

=cut

sub data_source {
  my ($self, @args) = @_;
  if (@args) {
    $self->{_data_source} = $args[0];
  }

  return $self->{_data_source};
}


=head2 username

  Arg [1]    : None

  Example    : $db->username
  Description: Return the current database user name.
  Returntype : string

=cut

sub username {
  my ($self, @args) = @_;
  if (@args) {
    $self->{_username} = $args[0];
  }

  return $self->{_username};
}

=head2 password

  Arg [1]    : None

  Example    : $db->password
  Description: Return the current database password.
  Returntype : string or undef

=cut

sub password {
  my ($self, @args) = @_;
  if (@args) {
    $self->{_password} = $args[0];
  }

  return $self->{_password};
}

=head2 log

  Arg [1]    : None

  Example    : $db->log
  Description: Return the current logger.
  Returntype : Logger object

=cut

sub log {
  my ($self, @args) = @_;
  if (@args) {
    $self->{_log} = $args[0];
  }

  my $log;
  if ($self->{_log}) {
    $log = $self->{_log};
  }
  else {
    $log = Log::Log4perl->get_logger('npg');
    $log->logcluck("Attempted to use a null Log4perl logger");
  }

  return $log;
}


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
