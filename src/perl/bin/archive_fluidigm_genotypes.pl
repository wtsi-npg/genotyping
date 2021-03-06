#!/usr/bin/env perl

package main;

use strict;
use warnings;
use Cwd qw/cwd/;
use DateTime;
use Getopt::Long;
use Log::Log4perl qw(:levels);
use Pod::Usage;
use WTSI::DNAP::Utilities::ConfigureLogger qw/log_init/;

use WTSI::NPG::Genotyping::Fluidigm::Archiver;
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'archive_fluidigm_genotypes');

our $VERSION = '';

run() unless caller();
sub run {

    my $days_ago;
    my $debug;
    my $dry_run;
    my $input_dir;
    my $irods_root;
    my $log4perl_config;
    my $output_dir;
    my $output_prefix;
    my $pigz_processes;
    my $verbose;

    GetOptions(
        'days-ago|days_ago=i'             => \$days_ago,
        'debug'                           => \$debug,
        'dry-run|dry_run'                 => \$dry_run,
        'help'                            => sub { pod2usage(-verbose => 2,
                                                             -exitval => 0) },
        'input-dir|input_dir=s'           => \$input_dir,
        'irods-root|irods_root=s'         => \$irods_root,
        'logconf=s'                       => \$log4perl_config,
        'output-dir|output_dir=s'         => \$output_dir,
        'output-prefix|output_prefix=s'   => \$output_prefix,
        'pigz-processes|pigz_processes=i' => \$pigz_processes,
        'verbose'                         => \$verbose
    );

    unless ($irods_root) {
        pod2usage(-msg     => "An --irods-root argument is required\n",
                  -exitval => 2);
    }

    my @log_levels;
    if ($debug) { push @log_levels, $DEBUG; }
    if ($verbose) { push @log_levels, $INFO; }
    log_init(config => $log4perl_config,
             file   => $session_log,
             levels => \@log_levels);
    my $log = Log::Log4perl->get_logger('main');

    $input_dir ||= cwd();

    my %args = (irods_root => $irods_root,
                target_dir => $input_dir,
            );
    if ($days_ago) { $args{'days_ago'} = $days_ago; }
    if ($output_dir) { $args{'output_dir'} = $output_dir; }
    if ($output_prefix) { $args{'output_prefix'} = $output_prefix; }
    if ($pigz_processes) { $args{'pigz_processes'} = $pigz_processes; }
    my $archiver = WTSI::NPG::Genotyping::Fluidigm::Archiver->new(%args);

    my @dirs_to_archive = $archiver->find_directories_to_archive();
    $log->info("Found ", scalar(@dirs_to_archive),
               " path(s) eligible for archiving");
    if (@dirs_to_archive) {
        $log->info("Directory path(s) to archive: ",
                   join(", ", @dirs_to_archive));
        $archiver->add_to_archives(\@dirs_to_archive, $dry_run);
    }
}


__END__

=head1 NAME

archive_fluidigm_genotypes

=head1 SYNOPSIS


Options:

  --days_ago        Minimum time in days since last modification, for
                    directories to be archived. Optional, defaults to 90 days.
  --dry_run         Report which directories would be archived, but do not
                    actually archive them.
  --help            Display help.
  --input_dir       Directory to search for archivable Fluidigm data.
                    Optional, defaults to current working directory.
  --irods_root      Root path in iRODS to search for published Fluidigm
                    data. If a directory's publication to iRODS cannot be
                    confirmed, it will not be archived. Required.
  --logconf         A log4perl configuration file. Optional.
  --output_dir      Directory for output of .tar.gz files. Optional, defaults
                    to current working directory.
  --output_prefix   Prefix for output filenames, which will be of the form
                    [prefix]_yyyy-mm.tar.gz. Optional, defaults to 'fluidigm'.
  --pigz_processes  Number of processes to use for compressing/decompressing
                    existing archives with the pigz program. Optional,
                    defaults to 4.
  --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Searches a directory recursively for Fluidigm result directories that
were last modified more than the given number of days ago. (N.B. limits
search to 1 level of directories.) Any files identified
are added to .tar.gz archives. The archive files are created on a monthly
basis, with names of the form [prefix]_yyyy-mm.tar.gz.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
