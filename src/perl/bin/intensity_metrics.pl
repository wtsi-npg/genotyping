#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# find normalized magnitude and xy intensity difference for each sample
# input .sim binary format intensity file
# replaces xydiff.pl

use strict;
use warnings;
use Getopt::Long;
use WTSI::Genotyping::QC::SimFiles qw/writeIntensityMetrics/;

my ($help, $inPath, $outPathXY, $outPathMag, $probeNum, $probeDefault,
    $in, $outMag, $outXY);

$probeDefault = 10000;

GetOptions("help"         => \$help,
           "input:s"      => \$inPath, # optional
           "xydiff=s"     => \$outPathXY,
           "magnitude=s"  => \$outPathMag,
           "probes=s"     => \$probeNum,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--input           Input path in .sim format; if blank, use standard input.
--magnitude       Output path for normalised magnitudes (required)
--xydiff          Output path for xydiff (required)
--probes          Size of probe input block; default = $probeDefault
--help            Print this help text and exit
";
    exit(0);
}

$probeNum ||= $probeDefault; # number of probes to read in at one time

if ($inPath) { open $in, "<", $inPath; }
else { $in = \*STDIN; }
open $outMag, ">", $outPathMag;
open $outXY, ">", $outPathXY;

writeIntensityMetrics($in, $outMag, $outXY, $probeNum);

foreach my $fh ($in, $outMag, $outXY) {
    close $fh || croak("Cannot close filehandle!");
}
