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
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::Database::Sequenom;
use WTSI::NPG::Genotyping::Sequenom::Publisher;

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

  GetOptions('config=s'    => \$config,
             'days=i'      => \$days,
             'days-ago=i'  => \$days_ago,
             'debug'       => \$debug,
             'dest=s'      => \$publish_dest,
             'help'        => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'   => \$log4perl_config,
             'verbose'     => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  $config ||= $DEFAULT_INI;
  $days ||= $DEFAULT_DAYS;
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
      (epoch => $now->epoch())->subtract(days => $days_ago);
  }
  else {
    $end = $now;
  }

  my $begin = DateTime->from_epoch
    (epoch => $end->epoch)->subtract(days => $days);

   my $sqdb = WTSI::NPG::Genotyping::Database::Sequenom->new
     (name    => 'mspec2',
      inifile => $config)->connect(RaiseError => 1);

  $log->info("Publishing from '", $sqdb->name, "' to '$publish_dest' ",
             "Sequenom results finished between ",
             $begin->iso8601, " and ", $end->iso8601);

  my $plate_names = $sqdb->find_finished_plate_names($begin, $end);
  $log->debug("Found ", scalar @$plate_names, " finished plates");

  foreach my $plate_name (@$plate_names) {
    my $publisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
    (publication_time => $now,
     plate_name       => $plate_name,
     sequenom_db      => $sqdb,
     logger           => $log);

    $publisher->publish($publish_dest);
  }

  return 0;
}


__END__

=head1 NAME

publish_sequenom_genotypes

=head1 SYNOPSIS


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
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Searches for finished Sequenom plates that have been modified within
the n days prior to a specific time and creates a CSV file of results
for each well. Any results identified are published to iRODS with
metadata obtained from the Sequenom LIMS.

The CSV files contain the following information as columns, identified
by a header row:

  ALLELE
  ASSAY_ID
  CHIP
  CUSTOMER
  EXPERIMENT
  GENOTYPE_ID
  HEIGHT
  MASS
  PLATE
  PROJECT
  SAMPLE_ID
  STATUS
  WELL_POSITION

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
