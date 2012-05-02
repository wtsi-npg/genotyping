#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

# invoke R script to do improved gender check
# script writes revised sample_xhet_gender.txt, and png plot of mixture model, to given output directory

use strict;
use warnings;
use Getopt::Long;
use QCPlotShared; # must have path to QCPlot* modules in PERL5LIB
use QCPlotTests;

my ($inDir, $outDir, $title, $help, $prefix, $clip, $trials, $sanityCancel, $sanityOpt);

GetOptions("input_dir=s"            =>  \$inDir,
	   "output_dir=s"           =>  \$outDir,
	   "title=s"                =>  \$title,
	   "file_prefix=s"          =>  \$prefix,
	   "cancel_sanity_check"    =>  \$sanityCancel,
	   "clip=f"                 =>  \$clip,
	   "trials=i"               =>  \$trials,
	   "h|help"                 =>  \$help);

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Script to do improved gender check by fitting a mixture model to xhet data.

Options:
--input_dir=PATH      Input directory containing sample_xhet_gender.txt file
--output_dir=PATH     Output directory 
--title=TITLE         Title for model summary plot
--file_prefix=NAME    Prefix for output file names
--cancel_sanity_check Cancel sanity-checking on model
--trials=INTEGER      Number of trials used to obtain consensus mdoel
--help                Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

$inDir  ||= '.';
$outDir ||= $inDir;
$title  ||= "Untitled";
$prefix ||= "sample_xhet_gender_model";
$clip   ||= 0.01; # proportion of high xhet values to clip; default to 1%
$trials ||= 1;
$sanityCancel ||= 0;

if ($inDir !~ /\/$/) { $inDir .= '/'; }
if ($outDir !~ /\/$/) { $outDir .= '/'; }

my $inPath = $inDir.'sample_xhet_gender.txt';
if ((not -r $inDir)||(not -d $inDir)) {
    die "ERROR: Input directory $inDir not accessible: $!";
} elsif (not -r $inPath) {
    die "ERROR: Cannot read input path $inPath: $!";
}
my $textPath = $outDir.$prefix.'.txt';
my $plotPath = $outDir.$prefix.'.png';
if ($sanityCancel) { $sanityOpt='FALSE'; }
else { $sanityOpt='TRUE'; }
my $summaryPath = $outDir.$prefix.'_summary.txt';
my $cmd = join(' ', ($QCPlotShared::RScriptPath, $QCPlotShared::parentDir.'/check_xhet_gender.R',
		     $inPath, $textPath, $plotPath, $title, $sanityOpt, $clip, $trials, ">& ".$summaryPath) ); 
# $cmd uses csh redirect
my ($tests, $failures) = (0,0);
($tests, $failures) = QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
if ($failures == 0) { exit(0); }
else { exit(1); }
