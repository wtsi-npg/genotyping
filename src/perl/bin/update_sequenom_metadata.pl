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
use List::MoreUtils qw(natatime);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::iRODS;

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 4;

run() unless caller();

sub run {
  my $config;
  my $debug;
  my $log4perl_config;
  my $num_processes;
  my $publish_dest;
  my $verbose;
  my @filter_key;
  my @filter_value;

  GetOptions('config=s'       => \$config,
             'debug'          => \$debug,
             'dest=s'         => \$publish_dest,
             'filter-key=s'   => \@filter_key,
             'filter-value=s' => \@filter_value,
             'help'           => sub { pod2usage(-verbose => 2,
                                                 -exitval => 0) },
             'logconf=s'      => \$log4perl_config,
             'verbose'        => \$verbose);
  $config ||= $DEFAULT_INI;

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  unless (scalar @filter_key == scalar @filter_value) {
    pod2usage(-msg => "There must be equal numbers of filter keys and values\n",
              -exitval => 2);
  }

  my @filter;
  while (@filter_key) {
    push @filter, [pop @filter_key, pop @filter_value];
  }

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

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name   => 'sequencescape_warehouse_prod',
     inifile =>  $config)->connect(RaiseError           => 1,
                                   mysql_enable_utf8    => 1,
                                   mysql_auto_reconnect => 1);

  my $snpdb = WTSI::NPG::Genotyping::Database::SNP->new
    (name   => 'snp',
     inifile => $config)->connect(RaiseError => 1);

  my $irods = WTSI::NPG::iRODS->new(logger => $log);

  my @sequenom_data =
    $irods->find_objects_by_meta($publish_dest,
                                 [sequenom_plate => '%', 'like'],
                                 [sequenom_well  => '%', 'like'],
                                 [type           => 'csv'],
                                 @filter);
  my $total = scalar @sequenom_data;
  my $updated = 0;

  $log->info("Updating metadata on $total data objects in '$publish_dest'");

  foreach my $data_object (@sequenom_data) {
    eval {
      my $sdo = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
        ($irods, $data_object);
      $sdo->update_secondary_metadata($snpdb, $ssdb);
      ++$updated;
    };

    if ($@) {
      $log->error("Failed to update metadata for '$data_object': ", $@);
    }
    else {
      $log->info("Updated metadata for '$data_object': $updated of $total");
    }
  }

  $log->info("Updated metadata on $updated/$total data objects in ",
             "'$publish_dest'");

  $ssdb->disconnect();
  $snpdb->disconnect();
}


__END__

=head1 NAME

update_sequenom_metadata

=head1 SYNOPSIS


Options:

  --config        Load database configuration from a user-defined
                  .ini file. Optional, defaults to
                  $HOME/.npg/genotyping.ini
  --dest          The data destination root collection in iRODS.
  --filter-key    Additional filter to limit set of dataObjs acted on.
  --filter-value
  --help          Display help.
  --logconf       A log4perl configuration file. Optional.
  --verbose       Print messages while processing. Optional.

=head1 DESCRIPTION

Searches for published Sequenom experimental data in iRODS, identifies
the Sequenom plate from which it came by means of the sequenom_plate
and sequenom_well metadata and adds relevant sample metadata taken
from the Sequencescape warehouse. If the new metadata include study
information, this is used to set access rights for the data in iRODS.

This script requires access to the SNP database in order to function.

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
