#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Getopt::Long;
use Log::Log4perl;
use Pod::Usage;

use WTSI::NPG::Utilities::IO qw(maybe_stdin);
use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY);
use WTSI::NPG::iRODS qw(find_collections_by_meta
                        find_objects_by_meta
                        find_zone_name
                        list_groups
                        make_group_name
                        set_group_access
);

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = DEBUG, A1
   log4perl.logger.quiet             = ERROR, A2

   log4perl.appender.A1          = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8     = 1
   log4perl.appender.A1.stderr   = 0
   log4perl.appender.A1.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2          = Log::Log4perl::Appender::Screen
   log4perl.appender.A2.utf8     = 1
   log4perl.appender.A2.stderr   = 0
   log4perl.appender.A2.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.Filter   = F2

   log4perl.filter.F2               = Log::Log4perl::Filter::LevelRange
   log4perl.filter.F2.LevelMin      = WARN
   log4perl.filter.F2.LevelMax      = FATAL
   log4perl.filter.F2.AcceptOnMatch = true
);

my $log;

run() unless caller();

sub run {
  my $access_level;
  my $dry_run;
  my $log4perl_config;
  my $publish_root;
  my $studies_list;
  my $verbose;

  GetOptions('access=s'    => \$access_level,
             'dry-run'     => \$dry_run,
             'help'        => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'   => \$log4perl_config,
             'root=s'      => \$publish_root,
             'studies=s'   => \$studies_list,
             'verbose'     => \$verbose);
  $access_level ||= 'read';

  my @valid_levels = ('null', 'read', 'write', 'own');
  unless (grep { /^$access_level$/ } @valid_levels) {
    pod2usage(-msg => "Invalid --access argument '$access_level'. Must be one of [" .
              join(', ', @valid_levels) . "]" ,
              -exitval => 2);
  }

  unless ($publish_root) {
    pod2usage(-msg => "A --root argument is required\n",
              -exitval => 2);
  }

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.publish');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    if ($verbose) {
      $log = Log::Log4perl->get_logger('npg.irods.publish');
    }
    else {
      $log = Log::Log4perl->get_logger('quiet');
    }
  }

  my $in = maybe_stdin($studies_list);

  while (my $study_id = <$in>) {
    chomp($study_id);
    $study_id =~ s/$\s+//;
    $study_id =~ s/\s+$//;

    set_access($study_id, $publish_root, 'object',
               $access_level, $dry_run);
    set_access($study_id, $publish_root, 'collection',
               $access_level, $dry_run);
  }
}

sub set_access {
  my ($study_id, $publish_root, $item_type, $access_level, $dry_run) = @_;
  my $plural = $item_type . 's';

  my $items;
  if ($item_type eq 'object') {
    $items = find_objects_by_meta($publish_root . '%',
                                  $STUDY_ID_META_KEY, $study_id);
  }
  elsif ($item_type eq 'collection') {
    $items = find_collections_by_meta($publish_root . '%',
                                      $STUDY_ID_META_KEY, $study_id);
  }
  else {
    $log->logconfess("Invalid item_type '$item_type'");
  }

  $log->debug("Searching for $plural in study '$study_id'");

  my $item_count = scalar @$items;
  my $set_count = 0;
  $log->debug("Found $item_count $plural in study '$study_id'");

  if ($item_count > 0) {
    my $group = make_group_name($study_id);
    $log->info("Setting $access_level access for group '$group' ",
               "for $item_count $plural in study '$study_id'");

    foreach my $item (@$items) {
      my $zone = find_zone_name($item);
      my $zoned_group = "$group#$zone";

      eval {
        unless ($dry_run) {
          set_group_access($access_level, $zoned_group, $item);
        }
      };

      if ($@) {
        $log->error("Failed to set $access_level access for group ",
                    "'$zoned_group' for $item_type '$item': ", $@);
      }
      else {
        ++$set_count;
        $log->debug("Set $access_level access for group '$zoned_group' for ",
                    "$item_type '$item'");
      }
    }

    $log->info("Done setting $access_level access for group '$group' ",
               "for $set_count/$item_count $plural in study '$study_id'");
  }
}


__END__

=head1 NAME

set_irods_group_access

=head1 SYNOPSIS

set_irods_group_access --root <irods collection> --access write \
  < list_of_study_ids

Options:

  --access      The iRODS access leve to set, one of [null, read,
                write, own]. Optional, defaults to 'read'.
  --dry-run     Run without executing any permissions changes. Optional.
  --root        The root collection in iRODS under which to search.
  --studies     The name of a file containing a list of Sequencescape
                study identifiers, one per line. Optional, defaults to
                STDIN.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Sets the group access permissions for all collections and/or
data objects under a root iRODS collection that have metadata
of study_id => <identifier>.

The list of study_ids may be supplied in a file, one per line, or
piped into STDIN (leading and/or trailing whitespace is ignored).

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
