#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# February 2014

# Collate QC metric files and merge into a single JSON file
# Optionally:
# * Apply thresholds and evaluate pass/fail status
# * Exclude failed samples in pipeline SQLite database
# Metrics appear in various formats in the QC directory

use strict;
use warnings;
use Carp;
use Cwd qw/abs_path/;
use Getopt::Long;
use Log::Log4perl qw(:levels);
use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::Genotyping::QC::Collator;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'collate_qc_results');
my $log;

my $config;
my $csv;
my $dbPath;
my $debug;
my $exclude;
my $help;
my $iniPath;
my $inputDir;
my $log4perl_config;
my $metricJson;
my $statusJson;
my $thresholds;
my $verbose;

GetOptions("config=s"      => \$config,
	   "csv=s"         => \$csv,
           "debug"         => \$debug,
	   "exclude"       => \$exclude,
	   "help"          => \$help,
	   "ini=s"         => \$iniPath,
           "input=s"       => \$inputDir,
	   "dbpath=s"      => \$dbPath,
           "logconf=s"     => \$log4perl_config,
	   "metrics=s"     => \$metricJson,
	   "status=s"      => \$statusJson,
	   "thresholds=s"  => \$thresholds,
	   "verbose"       => \$verbose,
       );

my @log_levels;
if ($debug) { push @log_levels, $DEBUG; }
if ($verbose) { push @log_levels, $INFO; }
log_init(config => $log4perl_config,
         file   => $session_log,
         levels => \@log_levels);
$log = Log::Log4perl->get_logger('main');

my $defaultStatusJson = 'qc_results.json';

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--input             Directory containing input files. Defaults to current working directory.
--status            Path for JSON output of metric values and pass/fail status. Defaults to $defaultStatusJson in current working directory.
--dbpath            Path to pipeline SQLite database file. Required.
--ini               Path to .ini file for SQLite database. Optional, defaults to $DEFAULT_INI.
--csv               Path for CSV output. Optional.
--metrics           Path for JSON output of metric values, without pass/fail status. Optional.
--config            Path to JSON file containing general configuration, including input filenames. Required.
--thresholds        Path to JSON file containing thresholds to determine pass/fail status for each sample. Optional; defaults to value of --config.
--exclude           Flag failed samples for exclusion in pipeline DB file.
--verbose           Print progress information to STDERR
--help              Print this help text and exit
";
    exit(0);
}

$inputDir ||= '.';
$statusJson ||= $defaultStatusJson;
$iniPath ||= $DEFAULT_INI;
$thresholds ||= $config;
$exclude ||= 0;

# validate command-line arguments
if (!$dbPath) { croak "Must supply a pipeline database path!"; }
elsif (!$config) { croak "Must supply a JSON config file!"; }
$dbPath = abs_path($dbPath); # required for pipeline DB object creation
if (!(-r $dbPath)) { croak "Cannot read pipeline database path $dbPath"; }
elsif (!(-d $inputDir)) { croak "Input $inputDir does not exist or is not a directory"; }
elsif (!(-r $config)) { croak "Cannot read config path $config"; }
elsif ($thresholds && !(-r $thresholds)) { croak "Cannot read thresholds path $thresholds"; }

my $collator = WTSI::NPG::Genotyping::QC::Collator->new(
    db_path  => $dbPath,
    ini_path => $iniPath,
    config_path => $config,
    filter_path => $thresholds,
);

$collator->collate($inputDir, $statusJson, $metricJson, $csv, $exclude);

__END__

=head1 NAME

collate_qc_results

=head1 DESCRIPTION

Collate genotyping QC results into a single JSON file

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015, 2016 Genome Research Limited.
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
