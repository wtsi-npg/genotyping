#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'ready_pipe');

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

run() unless caller();

sub run {
  my $config;
  my $dbfile;
  my $debug;
  my $log4perl_config;
  my $overwrite;
  my $verbose;

  GetOptions('config=s'  => \$config,
             'dbfile=s'  => \$dbfile,
             'debug'     => \$debug,
             'help'      => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s' => \$log4perl_config,
             'overwrite' => \$overwrite,
             'verbose'   => \$verbose);

  if ($log4perl_config) {
      Log::Log4perl::init($log4perl_config);
  } else {
      my $level;
      if ($debug) { $level = $DEBUG; }
      elsif ($verbose) { $level = $INFO; }
      else { $level = $ERROR; }
      my @log_args = ({layout => '%d %p %m %n',
                       level  => $level,
                       file   => ">>$session_log",
                       utf8   => 1},
                      {layout => '%d %p %m %n',
                       level  => $level,
                       file   => "STDERR",
                       utf8   => 1},
                  );
      Log::Log4perl->easy_init(@log_args);
  }
  my $log = Log::Log4perl->get_logger('main');

  $config ||= $DEFAULT_INI;
  my @initargs = (name      => 'pipeline',
                  inifile   => $config,
                  overwrite => $overwrite);

  if ($dbfile) {
    push @initargs, (dbfile => $dbfile);
  }

  my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (@initargs)->connect
      (RaiseError     => 1,
       sqlite_unicode => 1,
       on_connect_do  => 'PRAGMA foreign_keys = ON')->populate->disconnect;

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
  --logconf   A log4perl configuration file. Optional.
  --overwrite Overwrite any existing file, otherwise data dictionaries will
              be updated with new entries only.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Creates a genotyping pipeline run database and populates the data
dictionaries ready to add sets of samples for analysis.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012, 2013, 2014, 2015, 2016 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
