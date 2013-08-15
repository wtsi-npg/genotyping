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

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::Genotyping::Publication qw(publish_idat_files
                                          publish_gtc_files);
use WTSI::NPG::iRODS qw(collect_files
                        collect_dirs
                        modified_between);
use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri
                              get_publisher_name);

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
  my $source;
  my $type;
  my $verbose;

  GetOptions('config=s'   => \$config,
             'days=i'     => \$days,
             'days-ago=i' => \$days_ago,
             'debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'source=s'   => \$source,
             'type=s'     => \$type,
             'verbose'    => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }
  unless ($source) {
    pod2usage(-msg => "A --source argument is required\n",
              -exitval => 2);
  }
  unless ($type) {
    pod2usage(-msg => "A --type argument is required\n",
              -exitval => 2);
  }
  unless ($type =~ m{^idat$}msxi or $type =~ m{^gtc$}msxi) {
    pod2usage(-msg => "Invalid --type '$type'; expected one of [gtc, idat]\n",
              -exitval => 2);
  }

  $config ||= $DEFAULT_INI;
  $days ||= $DEFAULT_DAYS;
  $days_ago ||= 0;
  $type = lc($type);

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

  $log->info("Publishing '$type' from '$source' to '$publish_dest'",
             " last modified between ", $begin->iso8601,
             " and ", $end->iso8601);

  my $file_test = modified_between($begin->epoch(), $end->epoch());
  my $file_regex = qr{.($type)$}msxi;
  my $source_dir = abs_path($source);
  my $relative_depth = 2;

  my $ifdb = WTSI::NPG::Genotyping::Database::Infinium->new
    (name    => 'infinium',
     inifile => $config)->connect(RaiseError => 1);
  # $ifdb->log($log);

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config)->connect(RaiseError => 1,
                                  mysql_enable_utf8 => 1,
                                  mysql_auto_reconnect => 1);
  # $ssdb->log($log);

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $name = get_publisher_name($publisher_uri);

  $log->info("Publishing from '$source' to '$publish_dest' as ", $name);

  my @files;
  foreach my $dir (collect_dirs($source_dir, $file_test, $relative_depth)) {
    $log->debug("Checking directory '$dir'");
    my @found = collect_files($dir, $file_test, $relative_depth, $file_regex);
    $log->debug("Found " . scalar @found . " matching items in '$dir'");
    push(@files, @found);
  }

  # The above contains dupes due to the 2-level processing. Remove them.
  my @unique;
  my %seen;
  foreach my $file (@files) {
    if (!$seen{$file}) {
      push(@unique, $file);
      $seen{$file}++;
    }
  }

  if ($type eq 'idat') {
    publish_idat_files(\@unique, $creator_uri, $publish_dest, $publisher_uri,
                       $ifdb, $ssdb, $now);
  }
  elsif ($type eq 'gtc') {
    publish_gtc_files(\@unique, $creator_uri, $publish_dest, $publisher_uri,
                      $ifdb, $ssdb, $now);
  }
  else {
    $log->logcroak("Unable to publish unknown data type '$type'");
  }

  return 0;
}

__END__

=head1 NAME

publish_sample_data

=head1 SYNOPSIS

publish_sample_data [--config <database .ini file>] \
   [--days-ago <n>] [--days <n>] \
   --source <directory> --dest <irods collection> \
   --type <data type>

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
  --source      The root directory to search for sample data.
  --type        The data type to publish. One of [idat, gtc].
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Searches a directory recursively for idat or GTC sample data files
that have been modified within the n days prior to a specific time.
Any files identified are published to iRODS with metadata obtained from
LIMS.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
