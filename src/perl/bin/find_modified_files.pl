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

use WTSI::NPG::iRODS qw(collect_files
                        collect_dirs
                        modified_between);

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
  my $depth;
  my $log4perl_config;
  my $publish_dest;
  my $root;
  my $type;
  my $verbose;

  GetOptions('days=i'     => \$days,
             'days-ago=i' => \$days_ago,
             'debug'      => \$debug,
             'depth=i'    => \$depth,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'root=s'     => \$root,
             'type=s'     => \$type,
             'verbose'    => \$verbose);

  unless ($root) {
    pod2usage(-msg => "A --root argument is required\n",
              -exitval => 2);
  }
  unless ($type) {
    pod2usage(-msg => "A --type argument is required\n",
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

  $log->info("Finding file of type '$type' under '$root' ",
             "last modified between ", $begin->iso8601, " and ", $end->iso8601);

  my $file_test = modified_between($begin->epoch(), $end->epoch());
  my $file_regex = qr{.($type)$}msxi;
  my $root_dir = abs_path($root);

  my @found = collect_files($root_dir, $file_test, $depth, $file_regex);
  $log->debug("Found " . scalar @found . " matching items in '$root'");

  foreach my $file (@found) {
    print "$file\n";
  }
}



__END__

=head1 NAME

find_modified_files

=head1 SYNOPSIS

find_modified_files [--days-ago <n>] [--days <n>] \
   --root <directory> --type <data type>

Options:

  --days-ago    The number of days ago that the publication window ends.
                Optional, defaults to zero (the current day).
  --days        The number of days in the publication window, ending at
                the day given by the --days-ago argument. Any sample data
                modified during this period will be considered
                for publication. Optional, defaults to 7 days.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --root        The root directory to search for sample data.
  --type        The data type (file suffix) of file to print.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Searches a directory recursively files that have been modified within
the n days prior to a specific time and prints their path to
STDOUT.

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
