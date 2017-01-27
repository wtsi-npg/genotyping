#! /software/bin/perl

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
my $duplicates;
my $exclude;
my $help;
my $iniPath;
my $inputDir;
my $log4perl_config;
my $metricJson;
my $statusJson;
my $verbose;

GetOptions("config=s"      => \$config,
	   "csv=s"         => \$csv,
           "debug"         => \$debug,
           "duplicates=s"  => \$duplicates,
	   "exclude"       => \$exclude,
	   "help"          => \$help,
	   "ini=s"         => \$iniPath,
           "input=s"       => \$inputDir,
	   "dbpath=s"      => \$dbPath,
           "logconf=s"     => \$log4perl_config,
	   "metrics=s"     => \$metricJson,
	   "status=s"      => \$statusJson,
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
--duplicates        Path for JSON output of duplicate metric details. Optional; if absent, file will not be written.
--ini               Path to .ini file for SQLite database. Optional, defaults to $DEFAULT_INI.
--csv               Path for CSV output. Optional.
--metrics           Path for JSON output of metric values, without pass/fail status. Optional.
--config            Path to JSON file containing general configuration, including input filenames. Required.
--exclude           Flag failed samples for exclusion in pipeline DB file.
--verbose           Print progress information to STDERR
--help              Print this help text and exit
";
    exit(0);
}

$inputDir ||= '.';
$statusJson ||= $defaultStatusJson;
$iniPath ||= $DEFAULT_INI;
$exclude ||= 0;

# validate command-line arguments
if (!$dbPath) { croak "Must supply a pipeline database path!"; }
elsif (!$config) { croak "Must supply a JSON config file!"; }
$dbPath = abs_path($dbPath); # required for pipeline DB object creation
if (!(-r $dbPath)) { croak "Cannot read pipeline database path $dbPath"; }
elsif (!(-d $inputDir)) { croak "Input $inputDir does not exist or is not a directory"; }
elsif (!(-r $config)) { croak "Cannot read config path $config"; }

my $collator = WTSI::NPG::Genotyping::QC::Collator->new(
    db_path     => $dbPath,
    ini_path    => $iniPath,
    input_dir   => $inputDir,
    config_path => $config,
);

if (defined $metricJson) {
    $collator->writeMetricJson($metricJson);
}
if (defined $statusJson) {
    $collator->writePassFailJson($statusJson);
}
if (defined $csv) {
    $collator->writeCsv($csv);
}
if (defined $duplicates) {
    $collator->writeDuplicates($duplicates);
}
if ($exclude) {
    $collator->excludeFailedSamples();
}


__END__

=head1 NAME

collate_qc_results

=head1 DESCRIPTION

Collate genotyping QC results into a single object. Can write JSON and
CSV output, and exclude failed samples from the pipeline database.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015, 2016, 2017 Genome Research Limited.
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
