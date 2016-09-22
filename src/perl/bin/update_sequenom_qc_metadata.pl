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
use Try::Tiny;

use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'update_sequenom_qc_metadata');

our $VERSION = '';
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
  my @filter_key;
  my @filter_value;
  my $stdio;

  GetOptions('config=s'       => \$config,
             'days=i'         => \$days,
             'days-ago=i'     => \$days_ago,
             'debug'          => \$debug,
             'dest=s'         => \$publish_dest,
             'filter-key=s'   => \@filter_key,
             'filter-value=s' => \@filter_value,
             'help'           => sub { pod2usage(-verbose => 2,
                                                 -exitval => 0) },
             'logconf=s'      => \$log4perl_config,
             'verbose'        => \$verbose,
             ''               => \$stdio); # Permits a trailing '-' for STDIN
  $config   ||= $DEFAULT_INI;
  $days     ||= $DEFAULT_DAYS;
  $days_ago ||= 0;

  my @filter;

  if ($stdio) {
    if ($publish_dest or @filter_key) {
      pod2usage(-msg => "The --dest and --filter-key options are " .
                "incompatible with reading from STDIN\n",
                -exitval => 2);
    }
  }
  else {
    unless ($publish_dest) {
      pod2usage(-msg => "A --dest argument is required\n",
                -exitval => 2);
    }

    unless (scalar @filter_key == scalar @filter_value) {
      pod2usage(-msg => "There must be equal numbers of filter keys " .
                "and values\n",
                -exitval => 2);
    }

    while (@filter_key) {
      push @filter, [pop @filter_key, pop @filter_value];
    }
  }

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

  my $snpdb = WTSI::NPG::Genotyping::Database::SNP->new
    (name    => 'snp',
     inifile => $config)->connect(RaiseError => 1);

  my $irods = WTSI::NPG::iRODS->new();

  my @sequenom_data;
  if ($stdio) {
    while (my $line = <>) {
      chomp $line;
      push @sequenom_data, $line;
    }
  }
  else {
    $log->info("Updating QC metadata from for Sequenom plates whose status ",
               "has been changed between ", $begin->iso8601, " and ",
               $end->iso8601);

    my $plate_names = $snpdb->find_updated_plate_names($begin, $end);
    $log->debug("Found ", scalar @$plate_names, " updated plates");

    foreach my $plate_name (@$plate_names) {
      my @plate_data =
        $irods->find_objects_by_meta($publish_dest,
                                     [sequenom_plate => $plate_name],
                                     @filter);
      push @sequenom_data, @plate_data;
    }
  }

  my $total = scalar @sequenom_data;
  my $num_updated = 0;

  if ($stdio) {
    $log->info("Updating metadata on $num_updated/$total data objects in ",
               "file list");
  }
  else {
    $log->info("Updating metadata on $total data objects in '$publish_dest'");
  }

  foreach my $data_object (@sequenom_data) {
    try {
      my $sdo = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
        ($irods, $data_object);
      $sdo->update_qc_metadata($snpdb);

      $num_updated++;
      $log->info("Updated QC metadata for '$data_object': ",
                 "$num_updated of $total");
    } catch {
      $log->error("Failed to update QC metadata for '$data_object': ", $_);
    };
  }

  if ($stdio) {
    $log->info("Updated QC metadata on $num_updated/$total data objects in ",
               "file list");
  }
  else {
    $log->info("Updated QC metadata on $num_updated/$total data objects in ",
               "'$publish_dest'");
  }

  $snpdb->disconnect;
}

__END__

=head1 NAME

update_sequenom_qc_metadata

=head1 SYNOPSIS


Options:

  --config        Load database configuration from a user-defined
                  .ini file. Optional, defaults to
                  $HOME/.npg/genotyping.ini
  --days-ago      The number of days ago that the plate checking window
                  ends. Optional, defaults to zero (the current day).
  --days          The number of days in the checking window, ending at
                  the day given by the --days-ago argument. Any plate
                  modified during this period will be considered.
                  Optional, defaults to 7 days.
  --dest          The data destination root collection in iRODS.
  --filter-key    Additional filter to limit set of dataObjs acted on.
  --filter-value
  --help          Display help.
  --logconf       A log4perl configuration file. Optional.
  --verbose       Print messages while processing. Optional.

=head1 DESCRIPTION

Searches the SNP database for plates that have had their status
modified within the n days prior to a specific time, finds the data
for the plate in iRODS and then updates the QC metadata on those
files.

This script will read iRODS paths from STDIN as an alternative to
finding them via a metadata query. To do this, terminate the command
line with the '-' option. In this mode, the --dest, --filter-key and
--filter-value options are invalid.

This script requires access to the SNP database in order to function.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013, 2014, 2015, 2016 Genome Research Limited.
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
