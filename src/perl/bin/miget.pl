#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(trim user_session_log);

our $DEFAULT_ZONE = 'seq';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'miget');

my $embedded_conf = "
   log4perl.logger.npg.irods.subscribe = ERROR, A1, A2

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2           = Log::Log4perl::Appender::File
   log4perl.appender.A2.filename  = $session_log
   log4perl.appender.A2.utf8      = 1
   log4perl.appender.A2.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.syswrite  = 1
";

run() unless caller();

sub run {
  my $debug;
  my $dest;
  my $dry_run;
  my $list_key;
  my $verbose;
  my $zone;
  my @filter_key;
  my @filter_value;
  my $stdio;

  GetOptions('debug'            => \$debug,
             'dest=s'           => \$dest,
             'dry-run'          => \$dry_run,
             'filter-key=s'     => \@filter_key,
             'filter-value=s'   => \@filter_value,
             'help'             => sub { pod2usage(-verbose => 2,
                                                   -exitval => 0) },
             'list-key=s'       => \$list_key,
             'verbose'          => \$verbose,
             'zone=s',          => \$zone,
             ''                 => \$stdio);

  $dest ||= getcwd;
  $dest = abs_path($dest);

  $zone ||= $DEFAULT_ZONE;

  my @filter;

  unless ($stdio) {
    if ($list_key) {
      pod2usage(-msg     => "The list-key argument is only valid for list " .
                            "inputs\n",
                -exitval => 2);
    }

    unless (@filter_key || @filter_value) {
      pod2usage(-msg     => "At least one filter key and value must be " .
                            "supplied\n",
                -exitval => 2);
    }

    unless (scalar @filter_key == scalar @filter_value) {
      pod2usage(-msg     => "There must be equal numbers of filter keys " .
                            "and values\n",
                -exitval => 2);
    }

    while (@filter_key) {
      push @filter, [pop @filter_key, pop @filter_value];
    }
  }

  Log::Log4perl::init(\$embedded_conf);
  my $log = Log::Log4perl->get_logger('npg.irods.subscribe');

  if ($verbose || ($dry_run && !$debug)) {
    $log->level($INFO);
  }
  elsif ($debug) {
    $log->level($DEBUG);
  }

  my $irods = WTSI::NPG::iRODS->new(logger => $log);

  my @data_objects;
  my $num_objects;

  if ($stdio) {
    # The user has provided a list of paths or metadata values on
    # STDIN
    my @filter_list;
    while (my $line = <>) {
      chomp $line;
      push @filter_list, trim($line);
    }

    @filter_list = uniq @filter_list;

    if ($list_key) {
      # If the list key is provided, the list is of metadata values to
      # match against that key, to find data object paths
      foreach my $value (@filter_list) {
        my @objs = $irods->find_objects_by_meta("/$zone", [$list_key, $value]);
        push @data_objects, @objs;
      }

      $num_objects = scalar @data_objects;
      $log->info("Filter criteria matched $num_objects data objects ",
                 "in zone '$zone'");
    }
    else {
      # If the list key is not provided, the list is already of data
      # object paths
      @data_objects = @filter_list;

      $num_objects = scalar @data_objects;
      $log->info("$num_objects data objects provided as a list of paths");
    }
  }
  else {
    # The user has provided not a list of values, but a list of
    # metadata key-value pairs to find data object paths
    @data_objects = $irods->find_objects_by_meta("/$zone", @filter);

    $num_objects = scalar @data_objects;
    $log->info("Filter criteria matched $num_objects data objects ",
               "in zone '$zone'");
  }

  @data_objects = uniq @data_objects;
  $num_objects = scalar @data_objects;
  $log->info("$num_objects unique data objects to get");

  my $count = 0;
  foreach my $object (@data_objects) {
    $count++;
    if ($dry_run) {
      $log->info("Found '$object' $count/$num_objects");
    }
    else {
      $log->info("Getting '$object' $count/$num_objects");

      eval {
        $irods->get_object($object, $dest);
      };

      if ($@) {
        $log->error("Failed to get '$object': " . $@);
      }
    }
  }
}

__END__

=head1 NAME

miget - get multiple files from iRODS, matching metadata or paths

=head1 SYNOPSIS

  miget.pl --filter-key analysis_uuid --filter-value <UUID> \
           --filter-key type --filter-value gtc \
           --filter-key beadchip --filter-value 0123456789

  miget.pl --zone archive --list-key sample_id - < sample_ids.txt

  miget.pl --zone archive < sample_paths.txt

  miget.pl --filter-key study_id --filter-value 1234 --dry-run

Options:

  --dest            The directory into which fetched files will be saved.
                    Optional, defaults to the current directory.
  --dry-run         Report the files that would be fetched, but do not
                    fetch anything. Optional.
  --filter-key      iRODS metadata key to match during a search. Optional.
  --filter-value    iRODS metadata value to match during a search. Optional.
  --help            Display help.
  --list-key        iRODS metadata key to match in list values. Optional.
  --verbose         Print messages while processing. Optional.
  --zone            iRODS zone to search. Optional, defaults to 'seq'.
  -                 Read values from STDIN.

=head1 DESCRIPTION

This script allows a multiple files to be downloaded from iRODS in one
operation. Files may be specified by iRODS metadata keys and values,
by an iRODS metadata key and a list of values on STDIN or by a list of
iRODS paths on STDIN.

Values provided in STDIN should be formatted one per line. Any leading
or trailing whitespace will be ignored.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
