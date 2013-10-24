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

use WTSI::NPG::iRODS qw(collect_files
                        collect_dirs
                        modified_between);

use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri
                              get_publisher_name);

use WTSI::NPG::Genotyping::Fluidigm::ExportFile;
use WTSI::NPG::Genotyping::Fluidigm::Publisher;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

our $DEFAULT_DAYS = 7;

run() unless caller();
sub run {
  my $days;
  my $days_ago;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $source;
  my $verbose;

  GetOptions('days=i'      => \$days,
             'days-ago=i'  => \$days_ago,
             'debug'       => \$debug,
             'dest=s'      => \$publish_dest,
             'help'        => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'   => \$log4perl_config,
             'source=s'    => \$source,
             'verbose'     => \$verbose);

  unless ($source) {
    pod2usage(-msg => "A --source argument is required\n",
              -exitval => 2);
  }

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

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

  my $now = DateTime->now();
  my $end;
  if ($days_ago > 0) {
    $end = DateTime->from_epoch
      (epoch => $now->epoch())->subtract(days => $days_ago);
  }
  else {
    $end = $now;
  }

  my $begin = DateTime->from_epoch
    (epoch => $end->epoch())->subtract(days => $days);

  $log->info("Publishing Fluidigm results to '$publish_dest'",
             " finished between ", $begin->iso8601,
             " and ", $end->iso8601);

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $name = get_publisher_name($publisher_uri);

  $log->info("Publishing to '$publish_dest' as ", $name);

  my $dir_test = modified_between($begin->epoch(), $end->epoch());
  my $dir_regex = qr{^[0-9]{10}$}msxi;
  my $source_dir = abs_path($source);
  my $relative_depth = 2;

  foreach my $dir (collect_dirs($source_dir, $dir_test, $relative_depth,
                                $dir_regex)) {

    my $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
      (directory => $dir);

    my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
      (creator_uri => $creator_uri,
       publisher_uri => $publisher_uri,
       publication_time => $now,
       resultset => $resultset);

    $publisher->publish($publish_dest);
  }
}

__END__

=head1 NAME

publish_fluidigm_genotypes

=head1 SYNOPSIS


Options:

  --days-ago    The number of days ago that the publication window ends.
                Optional, defaults to zero (the current day).
  --days        The number of days in the publication window, ending at
                the day given by the --days-ago argument. Any sample data
                modified during this period will be considered
                for publication. Optional, defaults to 7 days.
  --dest        The data destination root collection in iRODS.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --source      The root directory to search for sample data.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Searches a directory recursively for Fluidigm result directories that
have been modified within the n days prior to a specific time.
(N.B. limits search to 1 level of directories.) Any files identified
are published to iRODS with metadata obtained from the exported CSV
file contained in each directory.

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
