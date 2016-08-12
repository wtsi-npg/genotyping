#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::Infinium::AnalysisPublisher;
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'publish_infinium_analysis');

my $embedded_conf = "
   log4perl.logger.npg.irods.publish = ERROR, A1, A2

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2           = Log::Log4perl::Appender::File
   log4perl.appender.A2.filename  = $session_log
   log4perl.appender.A2.utf8      = 1
   log4perl.appender.A2.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.syswrite  = 1
";

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

our $EXIT_CLI_ARG = 3;
our $EXIT_CLI_VAL = 4;
our $EXIT_UPLOAD  = 5;

run() unless caller();

sub run {
  my $archive_root;
  my $config;
  my $dbfile;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $run_name;
  my $source;
  my $verbose;

  GetOptions('archive=s' => \$archive_root,
             'config=s'  => \$config,
             'dbfile=s'  => \$dbfile,
             'debug'     => \$debug,
             'dest=s'    => \$publish_dest,
             'help'      => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s' => \$log4perl_config,
             'run=s'     => \$run_name,
             'source=s'  => \$source,
             'verbose'   => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg     => "A --dest argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }
  unless ($run_name) {
    pod2usage(-msg     => "A --run argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }
  unless ($source) {
    pod2usage(-msg     => "A --source argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }

  if ($config && ! -e $config) {
    pod2usage(-msg     => "The config file '$config' does not exist\n",
              -exitval => $EXIT_CLI_VAL);
  }
  if ($dbfile && ! -e $dbfile) {
    pod2usage(-msg     => "The database file '$dbfile' does not exist\n",
              -exitval => $EXIT_CLI_VAL);
  }
  unless (-e $source) {
    pod2usage(-msg     => "No such source as '$source'\n",
              -exitval => $EXIT_CLI_VAL);
  }
  unless (-d $source) {
    pod2usage(-msg     => "The --source argument was not a directory\n",
              -exitval => $EXIT_CLI_VAL);
  }

  $config ||= $DEFAULT_INI;

  my $log;

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger();
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger();

    if ($verbose) {
      $log->level($INFO);
    }
    elsif ($debug) {
      $log->level($DEBUG);
    }
  }

  my $db = $dbfile;
  $db ||= 'configured database';
  $log->debug("Using $db using config from $config");

  my @init_args = (name    => 'pipeline',
                   inifile => $config);
  if ($dbfile) {
    push @init_args, (dbfile => $dbfile);
  }

  my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (@init_args)->connect
      (RaiseError     => 1,
       sqlite_unicode => 1,
       on_connect_do  => 'PRAGMA foreign_keys = ON');

  my $now = DateTime->now;

  $log->info("Publishing from '$source' to '$publish_dest'");
  my @publisher_args = (analysis_directory => $source,
                        pipe_db            => $pipedb,
                        publication_time   => $now,
                        run_name           => $run_name);
  if ($archive_root) {
    push @publisher_args, (sample_archive => $archive_root);
  }

  my $publisher = WTSI::NPG::Genotyping::Infinium::AnalysisPublisher->new
    (@publisher_args);
  my $analysis_uuid = $publisher->publish($publish_dest);

  if (defined $analysis_uuid) {
    print "New analysis UUID: ", $analysis_uuid, "\n";
  }
  else {
    $log->error('No analysis UUID generated; upload aborted because of errors.',
                ' Please raise an RT ticket or email ',
                'new-seq-pipe@sanger.ac.uk');
    exit $EXIT_UPLOAD;
  }
}

__END__

=head1 NAME

publish_infinium_analysis

=head1 SYNOPSIS

publish_infinium_analysis [--config <database .ini file>] \
   [--dbfile <SQLite file>] --run <pipeline run name> \
   --source <directory> --dest <irods collection> [--verbose]

Options:

  --archive     Search pattern matching root of samples archive.
                Optional, defaults to '/archive/GAPI/gen/infinium'
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
