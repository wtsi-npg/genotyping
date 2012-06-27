#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# find xy intensity difference from .sim binary format
# replaces /software/varinf/bin/illuminus_calling/intensity_delta_xy
# want mean xydiff by sample
# this version uses all SNPs; old pipeline used only chromosome 1

use strict;
use warnings;
use Getopt::Long;
use WTSI::Genotyping::QC::SimFiles;

my ($help, $inPath, $outPath, $fh, $out);

GetOptions("help"         => \$help,
	   "input:s"      => \$inPath, # optional
	   "output=s"     => \$outPath
    );

if ($help) {
    print STDERR "Usage: $0 [ options ]
Options:
--input           Input path in .sim format; if blank, use standard input.
--output          Output path (required)
--help            Print this help text and exit
";
    exit(0);
}

if ($inPath) { open $fh, "< $inPath"; }
else { $fh = \*STDIN; }
open $out, "> $outPath";
my $useProbes = 1000;
WTSI::Genotyping::QC::SimFiles::readWriteXYDiffs($fh, $out, $useProbes);
close $fh;
close $out;



