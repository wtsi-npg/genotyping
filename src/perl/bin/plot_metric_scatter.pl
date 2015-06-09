#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# September 2012

use strict;
use warnings;
use Carp;
use Getopt::Long;
use FindBin qw($Bin);
use Log::Log4perl qw(:easy);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultPipelineDBConfig
                                               defaultJsonConfig
                                               getPlateLocationsFromPath
                                               readMetricResultHash
                                               readQCMetricInputs);
use WTSI::NPG::Genotyping::QC::MetricScatterplots qw(runAllMetrics);

our $VERSION = '';

Log::Log4perl->easy_init($ERROR);
my $log = Log::Log4perl->get_logger("genotyping");

my ($qcDir, $outDir, $title, $help, $config, $gender, $dbpath, $inipath, 
    $resultpath, $maxBatch, $noIntensity);

GetOptions("qcdir=s"    => \$qcDir,
           "outdir=s"   => \$outDir,
           "title=s"    => \$title,
           "config=s"   => \$config,
           "dbpath=s"   => \$dbpath,
           "gender=s"   => \$gender,
           "inipath=s"  => \$inipath,
           "resultpath=s"  => \$resultpath,
           "no-intensity"  => \$noIntensity,
           "max"        => \$maxBatch,
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
--gender=PATH       Path to .txt file with gender thresholds
--qcdir=PATH        Directory for QC input
--outdir=PATH       Directory for output; defaults to qcdir
--no-intensity      Omit intensity metric (normalized signal magnitude)
--max               Maximum number of samples on any one plot
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

$qcDir ||= '.';
$outDir ||= $qcDir;
$dbpath ||= $qcDir."/genotyping.db";
$inipath ||= defaultPipelineDBConfig();
$resultpath ||=  $qcDir."/qc_results.json";
$gender ||=  $qcDir."/sample_xhet_gender_thresholds.txt";
$config ||= defaultJsonConfig($inipath);

my @paths = ($qcDir, $outDir, $dbpath, $inipath, $resultpath, $config);
foreach my $path (@paths) {
    if (!(-r $path)) { croak "Cannot read path $path"; }
}

runAllMetrics($qcDir, $outDir, $config, $gender, $dbpath, $inipath, 
              $resultpath, $maxBatch, $noIntensity);

