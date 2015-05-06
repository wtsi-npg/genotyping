use utf8;

package WTSI::NPG::Database;

use Config::IniFiles;
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

with 'WTSI::DNAP::Utilities::Loggable';

sub BUILD {
  my ($self) = @_;

  my $ini = Config::IniFiles->new(-file => $self->inifile);
  $self->ini($ini);
}

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
