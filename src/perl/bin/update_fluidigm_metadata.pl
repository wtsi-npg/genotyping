#!/usr/bin/env perl

package main;

use strict;
use warnings;
use Getopt::Long;
use Log::Log4perl qw(:levels);
use Pod::Usage;
use Try::Tiny;

use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::Database::MLWarehouse;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'update_fluidigm_metadata');

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 4;

run() unless caller();

sub run {
  my $config;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $verbose;
  my @filter_key;
  my @filter_value;
  my $stdio;

  GetOptions('config=s'       => \$config,
             'debug'          => \$debug,
             'dest=s'         => \$publish_dest,
             'filter-key=s'   => \@filter_key,
             'filter-value=s' => \@filter_value,
             'help'           => sub { pod2usage(-verbose => 2,
                                                 -exitval => 0) },
             'logconf=s'      => \$log4perl_config,
             'verbose'        => \$verbose,
             ''               => \$stdio); # Permits a trailing '-' for STDIN
  $config ||= $DEFAULT_INI;

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

  my @log_levels;
  if ($debug) { push @log_levels, $DEBUG; }
  if ($verbose) { push @log_levels, $INFO; }
  log_init(config => $log4perl_config,
           file   => $session_log,
           levels => \@log_levels);
  my $log = Log::Log4perl->get_logger('main');

  my $ssdb = WTSI::NPG::Database::MLWarehouse->new
    (name   => 'multi_lims_warehouse',
     inifile =>  $config)->connect(RaiseError           => 1,
                                   mysql_enable_utf8    => 1,
                                   mysql_auto_reconnect => 1);

  my $irods = WTSI::NPG::iRODS->new();
  my @fluidigm_data;

  if ($stdio) {
    while (my $line = <>) {
      chomp $line;
      push @fluidigm_data, $line;
    }
  }
  else {
    @fluidigm_data =
      $irods->find_objects_by_meta($publish_dest,
                                   [fluidigm_plate => '%', 'like'],
                                   [fluidigm_well  => '%', 'like'],
                                   [type           => 'csv'],
                                   @filter);
  }

  my $total = scalar @fluidigm_data;
  my $updated = 0;

  if ($stdio) {
    $log->info("Updating metadata on $updated/$total data objects in ",
               "file list");
  }
  else {
    $log->info("Updating metadata on $total data objects in '$publish_dest'");
  }

  foreach my $data_object (@fluidigm_data) {
    try {
      my $fdo = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
        ($irods, $data_object);
      $fdo->update_secondary_metadata($ssdb);
      ++$updated;
    } catch {
      $log->error("Failed to update metadata for '$data_object': ", $_);
    };

    $log->info("Updated metadata for '$data_object': $updated of $total");
  }

  if ($stdio) {
    $log->info("Updated metadata on $updated/$total data objects in ",
               "file list");
  }
  else {
    $log->info("Updated metadata on $updated/$total data objects in ",
               "'$publish_dest'");
  }
}

__END__

=head1 NAME

update_fluidigm_metadata

=head1 SYNOPSIS



Options:

  --config       Load database configuration from a user-defined .ini file.
                 Optional, defaults to $HOME/.npg/genotyping.ini
  --dest         The data destination root collection in iRODS.
  --filter-key   Additional filter to limit set of dataObjs acted on.
  --filter-value
  --help         Display help.
  --logconf      A log4perl configuration file. Optional.
  --verbose      Print messages while processing. Optional.

=head1 DESCRIPTION

Searches for published Fluidigm genotyping experimental data in iRODS,
identifies the Fluidigm plate from which it came by means of the
fluidigm_plate and fluidigm_well metadata and adds relevant sample
metadata taken from the Sequencescape warehouse. If the new metadata
include study information, this is used to set access rights for the
data in iRODS.

This script will read iRODS paths from STDIN as an alternative to
finding them via a metadata query. To do this, terminate the command
line with the '-' option. In this mode, the --dest, --filter-key and
--filter-value options are invalid.

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
