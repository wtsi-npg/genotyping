
package WTSI::Genotyping::Database;

use strict;
use warnings;

use Carp;
use Config::IniFiles;
use DBI;


=head2 new

  Arg [1]    : name => string
  Arg [2]    : inifile => string
  Example    : WTSI::Genotyping::Database::<some class>->new
                 (name => 'my_database', inifile => 'may_database.ini')
  Description: Returns a new database handle configured from an
               .ini-style file.
  Returntype : WTSI::Genotyping::Database
  Caller     : general

=cut

sub new {
   my $class = shift;

   my $self = {};
   bless($self, $class);
   $self->configure(@_);
   return $self;
}


=head2 configure

  Arg [1]    : name => string
  Arg [2]    : inifile => string
  Example    : $db->configure(name => 'my_database',
                              inifile => 'may_database.ini')
  Description: Configures an exisiting database handle from an
               .ini-style file.
  Returntype : WTSI::Genotyping::Database
  Caller     : constructor, general

=cut

sub configure {
  my $self = shift;
  my %args = @_;

  $self->name($args{name});
  $self->inifile($args{inifile});
  my $ini = Config::IniFiles->new(-file => $self->inifile);

  unless ($self->name) {
    croak("No data source name was defined.")
  }

  $self->data_source($ini->val($self->name, 'data_source'));
  $self->username($ini->val($self->name, 'username'));
  $self->password($ini->val($self->name, 'password'));

  return $self;
}


=head2 inifile

  Arg [1]    : None
  Example    : $db->inifile
  Description: Returns the current .ini-style file.
  Returntype : string
  Caller     : general

=cut

sub inifile {
  my $self = shift;
  if (@_) {
    $self->{_inifile} = shift;
  }

  return $self->{_inifile};
}


=head2 connect

  Arg [n]    : key => value
  Example    : my $dbh = $db->connect(AutoCommit => 0, RaiseError => 1)->dbh;
  Description: Connects to the configured database using DBI. Additional
               DBI connection arguments may be supplied as key => value
               pairs.
  Returntype : WTSI::Genotyping::Database
  Caller     : general

=cut

sub connect {
  my $self = shift;
  my %args = @_;

  unless ($self->{_dbh}) {
    $self->{_dbh} = DBI->connect($self->data_source,
                                 $self->username,
                                 $self->password,
                                 \%args);
  }

  return $self;
}


=head2 disconnect

  Arg [1]    : None
  Example    : $db->disconnect
  Description: Disconnects the database handle.
  Returntype : 
  Caller     : general

=cut

sub disconnect {
  my $self = shift;
  $self->dbh->disconnect;
}


=head2 is_connected

  Arg [1]    : None
  Example    : $db->is_connected
  Description: Returns true if the database handle is connected.
  Returntype : boolean
  Caller     : general

=cut

sub is_connected {
  my $self = shift;
  return defined $self->dbh && $self->dbh->ping;
}


=head2 dbh

  Arg [1]    : None
  Example    : $db->dbh
  Description: Returns the current database handle.
  Returntype : DBI handle
  Caller     : general

=cut

sub dbh {
  my $self = shift;
  return $self->{_dbh};
}


=head2 name

  Arg [1]    : None
  Example    : $db->name
  Description: Returns the current database name.
  Returntype : string
  Caller     : general

=cut

sub name {
  my $self = shift;
  if (@_) {
    $self->{_name} = shift;
  }

  return $self->{_name};
}


=head2 data_source

  Arg [1]    : None
  Example    : $db->data_source
  Description: Returns the current database data source.
  Returntype : string
  Caller     : general

=cut

sub data_source {
  my $self = shift;
  if (@_) {
    $self->{_data_source} = shift;
  }

  return $self->{_data_source};
}


=head2 username

  Arg [1]    : None
  Example    : $db->username
  Description: Returns the current database user name.
  Returntype : string
  Caller     : general

=cut

sub username {
  my $self = shift;
  if (@_) {
    $self->{_username} = shift;
  }

  return $self->{_username};
}

=head2 password

  Arg [1]    : None
  Example    : $db->password
  Description: Returns the current database password.
  Returntype : string or undef
  Caller     : general

=cut

sub password {
  my $self = shift;
  if (@_) {
    $self->{_password} = shift;
  }

  return $self->{_password};
}

1;


__END__

=head1 NAME

WTSI::Genotyping::Database

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
