#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::Genotyping::Database::Pipeline;

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

Log::Log4perl->easy_init($ERROR);

run() unless caller();

sub run {
  my $config;
  my $dbfile;
  my $overwrite;
  my $verbose;

  GetOptions('config=s' => \$config,
             'dbfile=s'=> \$dbfile,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'overwrite' => \$overwrite,
             'verbose' => \$verbose);

  $config ||= $DEFAULT_INI;

  my $pipedb = WTSI::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => $config,
     dbfile => $dbfile,
     overwrite => $overwrite)->connect
       (RaiseError => 1,
        on_connect_do => 'PRAGMA foreign_keys = ON')->populate->disconnect;

  if ($verbose) {
    my $db = $dbfile;
    $db ||= 'configured database';
    print STDERR "Created $db using config from $config\n";
  }

  return;
}


__END__

=head1 NAME

ready_pipe

=head1 SYNOPSIS

ready_pipe [--config <database .ini file>] [--dbfile <SQLite file>] \
   [--overwrite] [--verbose]

Options:

  --config    Load database configuration from a user-defined .ini file.
              Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile    The SQLite database file. If not supplied, defaults to the
              value given in the configuration .ini file.
  --help      Display help.
  --overwrite Overwrite any existing file, otherwise data dictionaries will
              be updated with new entries only.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Creates a genotyping pipeline run database and populates the data
dictionaries ready to add sets of samples for analysis.

=head1 METHODS

None

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
