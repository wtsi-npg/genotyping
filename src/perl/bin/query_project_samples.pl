#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::Genotyping::Infinium::SampleQuery;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $EXPECTED_IRODS_FILES = 3;
our @DATA_SOURCE_NAMES = qw(LIMS_ IRODS SS_WH);
our @HEADER_FIELDS = qw(data_source plate well sample infinium_beadchip
                        infinium_beadchip_section sequencescape_barcode);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'query_project_samples');

my $embedded_conf = "
   log4perl.logger.npg.irods.publish = WARN, A1, A2

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

my $log;

run() unless caller();

sub run {

    my $config;
    my $debug_level;
    my $header;
    my $limit;
    my $outpath;
    my $project;
    my $quiet;
    my $root;
    my $log4perl_config;
    my $verbose;

    GetOptions(
        'config=s'         => \$config,
        'debug'            => \$debug_level,
        'header'           => \$header,
        'help'             => sub { pod2usage(-verbose => 2,
                                              -exitval => 0) },
        'limit=i'          => \$limit,
        'logconf=s'        => \$log4perl_config,
        'out=s'            => \$outpath,
        'project=s'        => \$project,
        'quiet'            => \$quiet,
        'root=s'           => \$root,
        'verbose'          => \$verbose);

    $config         ||= $DEFAULT_INI;
    unless ($project) {
        pod2usage(-msg => "A --project argument is required\n", -exitval => 2);
    }
    if (defined($limit) && $limit < 0) {
        pod2usage(-msg => "--limit argument must be >= 0\n", -exitval => 2);
    }
    if ($log4perl_config) {
        Log::Log4perl::init($log4perl_config);
        $log = Log::Log4perl->get_logger();
    }
    else {
        Log::Log4perl::init(\$embedded_conf);
        $log = Log::Log4perl->get_logger();
        if ($debug_level) {
            $log->level($DEBUG);
        } elsif ($verbose) {
            $log->level($INFO);
        } elsif ($quiet) {
            $log->level($ERROR);
        }
    }

    my $ifdb = WTSI::NPG::Genotyping::Database::Infinium->new
        (name    => 'infinium',
         inifile => $config)->connect(RaiseError => 1);

    my $ssdb = WTSI::NPG::Database::Warehouse->new
        (name    => 'sequencescape_warehouse',
         inifile => $config)->connect(RaiseError           => 1,
                                   mysql_enable_utf8    => 1,
                                   mysql_auto_reconnect => 1);

    $root           ||= '/archive';
    if ($root !~ '^/') { $root = '/'.$root; }

    my $sample_query = WTSI::NPG::Genotyping::Infinium::SampleQuery->new
        (infinium_db      => $ifdb,
         sequencescape_db => $ssdb);

    $sample_query->run($project, $root, $outpath, $header, $limit);
}


__END__

=head1 NAME

query_project_samples

=head1 SYNOPSIS

query_project_samples [--config <database .ini file>] \
   [--header] [--limit <n>] [--help] [--out <path>] \
   --project <project name> [--quiet] [ --root <irods root> ] [--verbose]

Options:

  --config        Load database configuration from a user-defined .ini file.
                  Optional, defaults to $HOME/.npg/genotyping.ini
  --header        Print a header line at the start of output (if any).
                  Optional.
  --help          Display help.
  --limit         Maximum number of samples to query in iRODS and
                  SequenceScape. Optional.
  --out           Path for output, or - for STDOUT. Optional; if omitted, no
                  output will be written.
  --project       Name of an Infinium LIMS project to query. Required.
  --quiet         Do not write warning messages; only log more serious
                  errors. Optional.
  --root          Root path of iRODS zone to query. Optional, defaults
                  to /archive.
  --verbose       Print additional messages while processing. Optional.

=head1 DESCRIPTION

Program to retrieve information about all samples in a given project from
the Infinium LIMS, iRODS, and the SequenceScape Warehouse.

The given project name is used to query the LIMS and obtain a list of
samples. The details of each sample are then used to query iRODS and
SequenceScape.

Warnings will be raised if:
- The iRODS query does not return exactly three data objects (we expect
  two IDATs and one GTC)
- SequenceScape does not return results for a given sample

If the --out option is given, the program will output three lines for each
sample: One each for the LIMS, iRODS, and SequenceScape. Each line contains
comma-separated values, which may be empty if the information is not
available, or not relevant to a given data source. To also write a brief
description of each field, run with the --header option.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
