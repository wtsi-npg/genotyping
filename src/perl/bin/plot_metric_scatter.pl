#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# September 2012

use strict;
use warnings;
use Carp;
use Getopt::Long;
use FindBin qw($Bin);
use Log::Log4perl qw(:easy);
use WTSI::Genotyping::QC::QCPlotShared qw(defaultJsonConfig getPlateLocationsFromPath readMetricResultHash readQCMetricInputs $INI_FILE_DEFAULT);
use WTSI::Genotyping::QC::MetricScatterplots qw(runAllMetrics);

Log::Log4perl->easy_init($ERROR);
my $log = Log::Log4perl->get_logger("genotyping");

my ($qcDir, $outDir, $title, $help, $config, $dbpath, $inipath, $resultpath);

GetOptions(#"metric=s"   => \$metric,
           "qcdir=s"    => \$qcDir,
           "outdir=s"   => \$outDir,
           "title=s"    => \$title,
           "config=s"   => \$config,
           "dbpath=s"   => \$dbpath,
           "inipath=s"  => \$inipath,
           "resultpath=s"  => \$resultpath,
           "h|help"     => \$help);


if ($help) {
    print STDERR "Usage: $0 [ options ] 
Script to create scatterplots by plate for QC metrics.
Metrics include chrX heterozygosity (for gender check), identity, call rate, autosome heterozygosity, and intensity magnitude.

Appropriate input data must be supplied to STDIN.

Options:
--config=PATH       Path to .json config file; default read from --inipath
--dbpath=PATH       Path to pipeline database containing plate information
--inipath=PATH      Path to .ini file for pipeline database
--resultpath=PATH   Path to .json file with pipeline results
--qcdir=PATH        Directory for QC input
--outdir=PATH       Directory for output; defaults to qcdir
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

$qcDir ||= '.';
$outDir ||= $qcDir;
$dbpath ||= $qcDir."/genotyping.db";
$inipath ||= $INI_FILE_DEFAULT;
$resultpath ||=  $qcDir."/qc_results.json";
$config ||= defaultJsonConfig($inipath);
#if (!$metric) { croak "Must supply a --metric argument!"; }

my @paths = ($qcDir, $outDir, $dbpath, $inipath, $resultpath, $config);
foreach my $path (@paths) {
    if (!(-r $path)) { croak "Cannot read path $path"; }
}

runAllMetrics($qcDir, $outDir, $config, $dbpath, $inipath, $resultpath);

system("rm -f Rplots.pdf");
# remove empty R output; TODO find out how to suppress Rplots.pdf in R script
