#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use Getopt::Long;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::Genotyping::Infinium::Publisher;
use WTSI::NPG::Utilities qw(collect_files
                            collect_dirs
                            modified_between);

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 7;

run() unless caller();

sub run {
  my $config;
  my $days;
  my $days_ago;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $verbose;
  my $stdio;

  my @sources;

  GetOptions('config=s'   => \$config,
             'days=i'     => \$days,
             'days-ago=i' => \$days_ago,
             'debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'source=s'   => \@sources,
             'verbose'    => \$verbose,
             ''           => \$stdio); # Permits a trailing '-' for STDIN

  if ($stdio) {
    if (@sources) {
      pod2usage(-msg => "The --source option is " .
                "incompatible with reading from STDIN\n",
                -exitval => 2);
    }
  }
  else {
    unless (@sources) {
      pod2usage(-msg => "A --source argument is required\n",
                -exitval => 2);
    }
  }

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  $config   ||= $DEFAULT_INI;
  $days     ||= $DEFAULT_DAYS;
  $days_ago ||= 0;

  my $log;

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.publish');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger('npg.irods.publish');

    if ($verbose) {
      $log->level($INFO);
    }
    elsif ($debug) {
      $log->level($DEBUG);
    }
  }

  my $now = DateTime->now;
  my $end;
  if ($days_ago > 0) {
    $end = DateTime->from_epoch
      (epoch => $now->epoch)->subtract(days => $days_ago);
  }
  else {
    $end = $now;
  }

  my $ifdb = WTSI::NPG::Genotyping::Database::Infinium->new
    (name    => 'infinium',
     inifile => $config,
     logger  => $log)->connect(RaiseError => 1);

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config,
     logger  => $log)->connect(RaiseError           => 1,
                               mysql_enable_utf8    => 1,
                               mysql_auto_reconnect => 1);

  my $begin = DateTime->from_epoch
    (epoch => $end->epoch)->subtract(days => $days);
  my $file_test = modified_between($begin->epoch, $end->epoch);
  my $file_regex = qr{.(gtc|idat)$}i;
  my $relative_depth = 2;

  @sources = map { abs_path($_) } @sources;

  my @files;
  if ($stdio) {
    while (my $line = <>) {
      chomp $line;
      push @files, $line;
    }
  }
  else {
    foreach my $source (@sources) {
      foreach my $dir (collect_dirs($source, $file_test, $relative_depth)) {
        $log->debug("Checking directory '$dir'");
        my @found = collect_files($dir, $file_test, $relative_depth,
                                  $file_regex);
        $log->debug("Found ", scalar @found, " matching items in '$dir'");
        push @files, @found;
      }
    }
  }

  @files = uniq(@files);
  my $total = scalar @files;

  if ($stdio) {
    $log->info("Publishing $total files in file list");
  }
  else {
    $log->info("Publishing from ", join(", ", @sources),
               " to '$publish_dest' Infinium results last modified between ",
               $begin->iso8601, " and ", $end->iso8601);

    $log->debug("Publishing $total files");
  }

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (publication_time => $now,
     data_files       => \@files,
     infinium_db      => $ifdb,
     ss_warehouse_db  => $ssdb,
     logger           => $log);
  $publisher->publish($publish_dest);

  return 0;
}

__END__

=head1 NAME

publish_infinium_genotypes

=head1 SYNOPSIS

publish_infinium_genotypes [--config <database .ini file>]
   [--days-ago <n>] [--days <n>]
   --source <directory> --dest <irods collection>

Options:

  --config      Load database configuration from a user-defined .ini file.
                Optional, defaults to $HOME/.npg/genotyping.ini
  --days-ago    The number of days ago that the publication window ends.
                Optional, defaults to zero (the current day).
  --days        The number of days in the publication window, ending at
                the day given by the --days-ago argument. Any sample data
                modified during this period will be considered
                for publication. Optional, defaults to 7 days.
  --dest        The data destination root collection in iRODS.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --source      The root directory to search for sample data. Multiple
                source arguments may be given.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Searches one or more a directories recursively for idat and GTC sample
data files that have been modified within the n days prior to a
specific time.  Any files identified are published to iRODS with
metadata obtained from LIMS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012-2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
