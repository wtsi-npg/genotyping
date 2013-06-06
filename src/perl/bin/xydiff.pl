#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# find xy intensity difference from .sim binary format
# replaces /software/varinf/bin/illuminus_calling/intensity_delta_xy
# want mean xydiff by sample
# this version uses all SNPs; old pipeline used only chromosome 1

use strict;
use warnings;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::SimFiles;

my ($help, $inPath, $outPath, $fh, $out, $verbose, $probeNum);

GetOptions("help"         => \$help,
	   "input:s"      => \$inPath, # optional
	   "output=s"     => \$outPath,
	   "verbose"      => \$verbose
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--input           Input path in .sim format; if blank, use standard input.
--output          Output path (required)
--verbose         Print additional output to STDOUT
--help            Print this help text and exit
";
    exit(0);
}
$verbose ||= 0; # defaults to quiet mode
$probeNum ||= 10000; # number of probes to read in at one time

if ($inPath) { open $fh, "<", $inPath; }
else { $fh = \*STDIN; }
open $out, ">", $outPath;
WTSI::Genotyping::QC::SimFiles::readWriteXYDiffs($fh, $out, $verbose, $probeNum);
close $fh;
close $out;



