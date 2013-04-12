#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use File::Basename;
use File::Find;
use Getopt::Long;
use Log::Log4perl qw(:easy);;
use Net::LDAP;
use Pod::Usage;
use URI;
use UUID;

use WTSI::Genotyping qw(make_analysis_metadata
                        get_wtsi_uri
                        get_publisher_uri
                        get_publisher_name
                        publish_analysis_directory);

use WTSI::Genotyping::Database::Pipeline;


my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = INFO, A1
   log4perl.logger.quiet             = ERROR, A2

   log4perl.appender.A1          = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.stderr   = 0
   log4perl.appender.A1.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2          = Log::Log4perl::Appender::Screen
   log4perl.appender.A2.stderr   = 0
   log4perl.appender.A2.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.Filter   = F2

   log4perl.filter.F2               = Log::Log4perl::Filter::LevelRange
   log4perl.filter.F2.LevelMin      = WARN
   log4perl.filter.F2.LevelMax      = FATAL
   log4perl.filter.F2.AcceptOnMatch = true
);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

run() unless caller();

sub run {
  my $config;
  my $dbfile;
  my $log4perl_config;
  my $publish_dest;
  my $run_name;
  my $source;
  my $verbose;

  GetOptions('config=s'  => \$config,
             'dbfile=s'=> \$dbfile,
             'dest=s'    => \$publish_dest,
             'help'      => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s' => \$log4perl_config,
             'run=s'     => \$run_name,
             'source=s'  => \$source,
             'verbose'   => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 3);
  }
  unless ($run_name) {
    pod2usage(-msg => "A --run argument is required\n", -exitval => 3);
  }
  unless ($source) {
    pod2usage(-msg => "A --source argument is required\n",
              -exitval => 3);
  }

  unless (-e $source) {
    pod2usage(-msg => "No such source as '$source'\n",
              -exitval => 4);
  }
  unless (-d $source) {
    pod2usage(-msg => "The --source argument was not a directory\n",
              -exitval => 4);
  }

  $config ||= $DEFAULT_INI;

  my $log;

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.publish');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    if ($verbose) {
      $log = Log::Log4perl->get_logger('npg.irods.publish');
    }
    else {
      $log = Log::Log4perl->get_logger('quiet');
    }
  }

  my $now = DateTime->now();

  my $db = $dbfile;
  $db ||= 'configured database';
  $log->debug("Using $db using config from $config");

  my $pipedb = WTSI::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => $config,
     dbfile => $dbfile)->connect
       (RaiseError => 1,
        on_connect_do => 'PRAGMA foreign_keys = ON');

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $name = get_publisher_name($publisher_uri);

  $log->info("Publishing from '$source' to '$publish_dest'");

  print publish_analysis_directory($source, $creator_uri,
                                   $publish_dest, $publisher_uri,
                                   $pipedb, $run_name, $now);
  print "\n";
}


__END__

=head1 NAME

publish_analysis_data

=head1 SYNOPSIS

publish_analysis_data [--config <database .ini file>] \
   [--dbfile <SQLite file>] --run <pipeline run name> \
   --source <directory> --dest <irods collection> [--verbose]

Options:

  --config      Load database configuration from a user-defined .ini file.
                Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile      The SQLite database file. If not supplied, defaults to the
                value given in the configuration .ini file.
  --dest        The data destination root collection in iRODS.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --run         The pipeline run name in the database.
  --source      The root directory of the analysis.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Publishes an analysis directory to an iRODS collection. Adds
to the collection metadata describing the genotyping projects
analysed and a new UUID for the analysis. It also locates the
corresponding sample data in iRODS and cross-references it to
the analysis by adding the UUID to the sample metadata.


=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
