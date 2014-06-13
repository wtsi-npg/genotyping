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
use WTSI::NPG::iRODS;

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 14;

run() unless caller();

sub run {
  my $config;
  my $days;
  my $days_ago;
  my $debug;
  my $force;
  my $log4perl_config;
  my $project;
  my $publish_dest;
  my $verbose;
  my $stdio;

  GetOptions('config=s'   => \$config,
             'days=i'     => \$days,
             'days-ago=i' => \$days_ago,
             'debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'force'      => \$force,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'project=s'  => \$project,
             'verbose'    => \$verbose,
             ''           => \$stdio); # Permits a trailing '-' for STDIN

  if ($stdio && ($days || $days_ago)) {
    pod2usage(-msg => "The --days and --days-ago options are " .
              "incompatible with reading from STDIN\n",
              -exitval => 2);
  }
  if ($stdio && $project) {
    pod2usage(-msg => "The --project option is " .
              "incompatible with reading from STDIN\n",
              -exitval => 2);
  }
  if ($stdio && $force) {
    pod2usage(-msg => "The --force option is " .
              "incompatible with reading from STDIN\n",
              -exitval => 2);
  }
  if ($project && ($days || $days_ago)) {
    pod2usage(-msg => "The --days and --days-ago options are " .
              "incompatible with the --project option\n",
              -exitval => 2);
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

  my $begin = DateTime->from_epoch
    (epoch => $end->epoch)->subtract(days => $days);

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

  my @files;
  if ($stdio) {
    while (my $line = <>) {
      chomp $line;
      push @files, $line;
    }
  }
  else {
    my $irods = WTSI::NPG::iRODS->new(logger => $log);
    @files = find_files_to_publish($ifdb, $begin, $end, $project, $irods,
                                   $publish_dest, $force, $log);
  }

  @files = uniq @files;
  my $total = scalar @files;

  if ($stdio) {
    $log->info("Publishing $total files in file list");
  }
  elsif ($project) {
    $log->info("Publishing to '$publish_dest' Infinium results in project ",
               "'$project'");
  }
  else {
    $log->info("Publishing to '$publish_dest' Infinium results ",
               "scanned between ", $begin->iso8601, " and ", $end->iso8601);

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

sub find_files_to_publish {
  my ($ifdb, $begin, $end, $project, $irods, $publish_dest, $force, $log) = @_;

  my @samples;

  if ($project) {
    $log->debug("Finding scanned samples in project '$project' ...");
    @samples = @{$ifdb->find_project_samples($project)};
  }
  else {
    $log->debug("Finding samples scanned between ", $begin->iso8601,
                " and ", $end->iso8601);
    @samples = @{$ifdb->find_scanned_samples_by_date($begin, $end)};
  }

  my @to_publish;

  foreach my $if_sample (@samples) {
    my $chip    = $if_sample->{beadchip};
    my $section = $if_sample->{beadchip_section};
    my $red     = $if_sample->{idat_red_path};
    my $grn     = $if_sample->{idat_grn_path};
    # May be NULL if not called yet, or if a methylation chip
    my $gtc     = $if_sample->{gtc_path};

    my @files;
    foreach my $file (($red, $grn, $gtc)) {
      if ($file) {
        push @files, _map_netapp_path($file);
      }
    }

    if ($force) {
      push @to_publish, @files;
    }
    else {
      my @data_objects = $irods->find_objects_by_meta
        ($publish_dest,
         ['beadchip', $chip],
         ['beadchip_section', $section]);

      my $num_files        = scalar @files;
      my $num_data_objects = scalar @data_objects;

      $log->info("Beadchip '$chip' section '$section' data objects published ",
                 "previously: $num_data_objects/$num_files");

      if ($num_data_objects < $num_files) {
        push @to_publish, @files;
      }
    }
  }

  return @to_publish;
}

sub _map_netapp_path {
  my ($netapp_path) = @_;

  my $mapped_path = '';

  if ($netapp_path) {
    $mapped_path = $netapp_path;
    $mapped_path =~ s{\\}{/}gmsx;
    $mapped_path =~ s{//}{/}msx;
    $mapped_path =~ s{netapp\d[ab]/illumina_geno(\d)}{nfs/new_illumina_geno0$1}msx;

    $mapped_path =~ s{_r(\d+)c(\d+)_}{_R$1C$2_}msx;
    $mapped_path =~ s{grn}{Grn}msx;
    $mapped_path =~ s{red}{Red}msx;
  }

  return $mapped_path;
}

__END__

=head1 NAME

publish_infinium_genotypes

=head1 SYNOPSIS

publish_infinium_genotypes [--config <database .ini file>]
   [--days-ago <n>] [--days <n>] --dest <irods collection> [--force]
   [--project <project name>] [ - < STDIN]

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
  --project     The name of a genotyping project in the Infinium LIMS.
  --verbose     Print messages while processing. Optional.
  -             Read from STDIN.

=head1 DESCRIPTION

Searches one or more a directories recursively for idat and GTC sample
data files that have been modified within the n days prior to a
specific time.  Any files identified are published to iRODS with
metadata obtained from LIMS.

This script also accepts lists of specific files on STDIN, as an
alternative to searching for files by modification time. To do this,
terminate the command line with the '-' option. In this mode, the
--source, --days and --days-ago options are invalid.

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
