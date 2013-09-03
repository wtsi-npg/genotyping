#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# find normalized magnitude and xy intensity difference for each sample
# uses inline C for greater speed

use strict;
use warnings;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::SimFiles qw/writeIntensityMetrics/;

my ($help, $inPath, $outPathXY, $outPathMag, $outMag, $outXY, $verbose);

GetOptions("help"         => \$help,
           "input=s"      => \$inPath,
           "xydiff=s"     => \$outPathXY,
           "magnitude=s"  => \$outPathMag,
           "verbose"      => \$verbose,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--input=PATH      Input path in .sim format
--magnitude=PATH  Output path for normalised magnitudes (required)
--xydiff=PATH     Output path for xydiff (required)
--verbose         Print additional information to standard output
--help            Print this help text and exit
";
    exit(0);
} elsif (!($inPath && $outPathXY && $outPathMag)) {
    print STDERR "Incorrect arguments; run with --help for usage.\n";
    exit(1);
}

$verbose||=0;
writeIntensityMetrics($inPath, $outPathMag, $outPathXY, $verbose);
