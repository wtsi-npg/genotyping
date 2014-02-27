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
use WTSI::NPG::Genotyping::QC::Collation qw(collate);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

my ($inputDir, $metricJson, $statusJson, $csv, $iniPath, $dbPath, $thresholds, 
    $config, $exclude, $help, $verbose);

GetOptions("input=s"       => \$inputDir,
	   "dbpath=s"      => \$dbPath,
	   "metrics=s"     => \$metricJson,
	   "status=s"      => \$statusJson,
	   "csv=s"         => \$csv,
	   "ini=s"         => \$iniPath,
	   "config=s"      => \$config,
	   "thresholds=s"  => \$thresholds,
	   "exclude"       => \$exclude,
	   "help"          => \$help,
	   "verbose"       => \$verbose,
    );

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

# assign 0 (ie. false) to the optional reference to a list of metric names
collate($inputDir, $config, $thresholds, $dbPath, $iniPath, $statusJson, 
	$metricJson, $csv, $exclude, 0, $verbose);
