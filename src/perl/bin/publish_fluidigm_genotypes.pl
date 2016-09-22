#!/usr/bin/env perl

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;
use Try::Tiny;

use WTSI::NPG::Database::MLWarehouse;
use WTSI::NPG::Genotyping::Fluidigm::ExportFile;
use WTSI::NPG::Genotyping::Fluidigm::Publisher;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;
<<<<<<< HEAD
use WTSI::NPG::Utilities::Collector;

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);
=======
use WTSI::NPG::Utilities qw(user_session_log);
use WTSI::NPG::Utilities::Collector;
>>>>>>> logger_init

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 7;
our $DEFAULT_REFERENCE_PATH = '/seq/fluidigm/multiplexes';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'publish_fluidigm_genotypes');

run() unless caller();
sub run {
  my $config;
  my $days;
  my $days_ago;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $reference_path;
  my $source;
  my $verbose;

  GetOptions('config=s'         => \$config,
             'days=i'           => \$days,
             'days-ago=i'       => \$days_ago,
             'debug'            => \$debug,
             'dest=s'           => \$publish_dest,
             'help'             => sub { pod2usage(-verbose => 2,
                                                   -exitval => 0) },
             'logconf=s'        => \$log4perl_config,
             'reference-path=s' => \$reference_path,
             'source=s'         => \$source,
             'verbose'          => \$verbose);

  unless ($source) {
    pod2usage(-msg     => "A --source argument is required\n",
              -exitval => 2);
  }

  unless ($publish_dest) {
    pod2usage(-msg     => "A --dest argument is required\n",
              -exitval => 2);
  }

  $config         ||= $DEFAULT_INI;
  $days           ||= $DEFAULT_DAYS;
  $days_ago       ||= 0;
  $reference_path ||= $DEFAULT_REFERENCE_PATH;;

  if ($log4perl_config) {
      Log::Log4perl::init($log4perl_config);
  }
  else {
      my $level;
      if ($debug) { $level = $DEBUG; }
      elsif ($verbose) { $level = $INFO; }
      else { $level = $ERROR; }
      my @log_args = ({layout => '%d %p %m %n',
                       level  => $level,
                       file     => ">>$session_log",
                       utf8   => 1},
                      {layout => '%d %p %m %n',
                       level  => $level,
                       file   => "STDERR",
                       utf8   => 1},
                  );
      Log::Log4perl->easy_init(@log_args);
  }
  my $log = Log::Log4perl->get_logger('main');

  my $whdb = WTSI::NPG::Database::MLWarehouse->new
    (name    => 'multi_lims_warehouse',
     inifile =>  $config)->connect(RaiseError           => 1,
                                   mysql_enable_utf8    => 1,
                                   mysql_auto_reconnect => 1);

  my $now = DateTime->now;
  my $end;
  if ($days_ago > 0) {
    $end = DateTime->from_epoch
      (epoch => $now->epoch)->subtract(days => $days_ago);
  }
  else {
    $end = $now;
  }
  my $begin = DateTime->from_epoch
    (epoch => $end->epoch)->subtract(days => $days);

  my $source_dir = abs_path($source);
  $log->info("Publishing from '$source_dir' to '$publish_dest' Fluidigm ",
             "results finished between ",
             $begin->iso8601, " and ", $end->iso8601);
  $log->info("Using reference path '$reference_path'");

  my $collector = WTSI::NPG::Utilities::Collector->new(
      root  => $source_dir,
      depth => 2,
      regex => qr{^\d{10}$}msxi,
  );
  my @dirs = $collector->collect_dirs_modified_between($begin->epoch,
                                                       $end->epoch);
  my $total = scalar @dirs;
  my $num_published = 0;

  $log->debug("Publishing $total Fluidigm data directories in '$source_dir'");

  foreach my $dir (@dirs) {
    try {
      my $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
        (directory => $dir);

      my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
        (publication_time => $now,
         resultset        => $resultset,
         reference_path   => $reference_path,
         warehouse_db     => $whdb,
         );

      $publisher->publish($publish_dest);
      $num_published++;
    } catch {
      $log->error("Failed to publish '$dir': ", $_);
    };

    $log->info("Published '$dir': $num_published of $total");
  }
}

__END__

=head1 NAME

publish_fluidigm_genotypes

=head1 SYNOPSIS


Options:

  --days-ago        The number of days ago that the publication window
                    ends. Optional, defaults to zero (the current day).
  --days            The number of days in the publication window, ending
                    at the day given by the --days-ago argument. Any sample
                    data modified during this period will be considered
                    for publication. Optional, defaults to 7 days.
  --dest            The data destination root collection in iRODS.
  --help            Display help.
  --logconf         A log4perl configuration file. Optional.
  --reference-path  Provides an iRODS path (and therfore zone hint) as
                    to where to look for SNP set manifests. Optional,
                    defaults to 'seq'.
  --source          The root directory to search for sample data.
  --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Searches a directory recursively for Fluidigm result directories that
have been modified within the n days prior to a specific time.
(N.B. limits search to 1 level of directories.) Any files identified
are published to iRODS with metadata obtained from the exported CSV
file contained in each directory.

The SNPs reported in the Fluidigm data files are matched against
reference manifests of SNPs stored in iRODS in order to tell which set
of SNPs has been analysed.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015, 2016 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
