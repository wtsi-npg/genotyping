#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use DateTime;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Sequenom;
use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::Sequenom::Publisher;

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 30;

run() unless caller();

sub run {
  my $config;
  my $days;
  my $days_ago;
  my $debug;
  my $force;
  my $log4perl_config;
  my $publish_dest;
  my $verbose;
  my $stdio;

  GetOptions('config=s'    => \$config,
             'days=i'      => \$days,
             'days-ago=i'  => \$days_ago,
             'debug'       => \$debug,
             'dest=s'      => \$publish_dest,
             'force'       => \$force,
             'help'        => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'   => \$log4perl_config,
             'verbose'     => \$verbose,
             ''            => \$stdio); # Permits a trailing '-' for STDIN

  if ($stdio && ($days || $days_ago)) {
    pod2usage(-msg => "The --days and --days-ago options are " .
              "incompatible with reading from STDIN\n",
              -exitval => 2);
  }
  if ($stdio && $force) {
    pod2usage(-msg => "The --force option is " .
              "incompatible with reading from STDIN\n",
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
      (epoch => $now->epoch())->subtract(days => $days_ago);
  }
  else {
    $end = $now;
  }

  my $begin = DateTime->from_epoch
    (epoch => $end->epoch)->subtract(days => $days);

  my $sqdb = WTSI::NPG::Genotyping::Database::Sequenom->new
    (name    => 'mspec2',
     inifile => $config,
     logger  => $log)->connect(RaiseError => 1);

  my $snpdb = WTSI::NPG::Genotyping::Database::SNP->new
    (name    => 'snp',
     inifile => $config,
     logger  => $log)->connect(RaiseError => 1);

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name   => 'sequencescape_warehouse',
     inifile =>  $config)->connect(RaiseError           => 1,
                                   mysql_enable_utf8    => 1,
                                   mysql_auto_reconnect => 1);

  my @plate_names;
  if ($stdio) {
    while (my $line = <>) {
      chomp $line;
      push @plate_names, $line;
    }
  }
  elsif ($force) {
    @plate_names = @{$sqdb->find_finished_plate_names($begin, $end)};
  }
  else {
    my $irods = WTSI::NPG::iRODS->new(logger => $log);
    @plate_names = find_plates_to_publish($sqdb, $begin, $end, $irods,
                                          $publish_dest, $log);
  }

  my $total = scalar @plate_names;
  my $published = 0;

  if ($stdio) {
    $log->info("Publishing $total plates in plate list");
  }
  else {
    my $op = $force ? "Force publishing" : "Publishing";

    $log->info("$op from '", $sqdb->name, "' to '$publish_dest' ",
               "Sequenom results finished between ",
               $begin->iso8601, " and ", $end->iso8601);
    $log->debug("$op $total finished plates");
  }

  foreach my $plate_name (@plate_names) {
    my $publisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
      (publication_time => $now,
       plate_name       => $plate_name,
       sequenom_db      => $sqdb,
       snp_db           => $snpdb,
       ss_warehouse_db  => $ssdb,
       logger           => $log);

    $publisher->publish($publish_dest);
    $published++;
    $log->debug("Published $published/$total plates");
  }

  return 0;
}

# Compare with data in iRODS. If any plate has less than the full
# complement of files in iRODS (one per well) then re-publish the
# plate.
sub find_plates_to_publish {
  my ($sqdb, $begin, $end, $irods, $publish_dest, $log) = @_;

  my @to_publish;

  my @plate_names = @{$sqdb->find_finished_plate_names($begin, $end)};
  foreach my $plate_name (@plate_names) {
    my @wells = @{$sqdb->find_plate_result_wells($plate_name)};

    my @data_objects = $irods->find_objects_by_meta
      ($publish_dest,
       ['sequenom_plate', $plate_name],
       ['sequenom_well', '%', 'like']);

    my $num_wells        = scalar @wells;
    my $num_data_objects = scalar @data_objects;

    $log->info("Plate '$plate_name' data objects published previously: ",
               "$num_data_objects/$num_wells");

    if ($num_data_objects < $num_wells) {
      push @to_publish, $plate_name;
    }
  }

  return @to_publish;
}

__END__

=head1 NAME

publish_sequenom_genotypes

=head1 SYNOPSIS

publish_sequenom_genotypes [--config <database .ini file>]
   [--days-ago <n>] [--days <n>] --dest <irods collection> [ - < STDIN]

Options:

  --config      Load database configuration from a user-defined .ini file.
                Optional, defaults to $HOME/.npg/genotyping.ini
  --days-ago    The number of days ago that the publication window ends.
                Optional, defaults to zero (the current day).
  --days        The number of days in the publication window, ending at
                the day given by the --days-ago argument. Any sample data
                modified during this period will be considered
                for publication. Optional, defaults to 30 days.
  --dest        The data destination root collection in iRODS.
  --force       Publish files, even if they are already in iRODS.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --verbose     Print messages while processing. Optional.
  -             Read from STDIN.

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

This script will read plate names from STDIN as an alternative to
finding them via a LIMS query. To do this, terminate the command line
with the '-' option. In this mode, the --days and --days-ago options
are invalid.

Note that the date the plate is marked 'finished' in the LIMS is not
the date its analysis was finished(!) It is actually the date that a
"virtual plate" was first added to the LIMS. Therefore the --days and
--days-ago arguments should be used with caution. The default number
of days in thhe publication window is 30 to attempt to capture all
results, at the cost of much redundant activity.


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
