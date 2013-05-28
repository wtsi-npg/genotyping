#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# find normalized magnitude and xy intensity difference for each sample
# input .sim binary format intensity file
# replaces xydiff.pl

# may take a few hours for full-sized projects, log provides progress report

use strict;
use warnings;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::SimFiles qw/writeIntensityMetrics/;

my ($help, $inPath, $outPathXY, $outPathMag, $probeNum, $probeDefault,
    $logPath, $logDefault, $outMag, $outXY);

$probeDefault = 5000;
$logDefault = "./intensity_metrics.log";

GetOptions("help"         => \$help,
           "input:s"      => \$inPath, # optional
           "log=s"        => \$logPath,
           "xydiff=s"     => \$outPathXY,
           "magnitude=s"  => \$outPathMag,
           "probes=s"     => \$probeNum,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--input           Input path in .sim format; if blank, use standard input.
--log             Log path; defaults to $logDefault
--magnitude       Output path for normalised magnitudes (required)
--xydiff          Output path for xydiff (required)
--probes          Size of probe input block; default = $probeDefault
--help            Print this help text and exit
";
    exit(0);
}

$logPath ||= $logDefault;
$probeNum ||= $probeDefault; # number of probes to read in at one time

writeIntensityMetrics($inPath, $outPathMag, $outPathXY, $logPath, $probeNum);
