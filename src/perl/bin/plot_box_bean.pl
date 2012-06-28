#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create boxplots of cr, het rate, xydiff
# also creates beanplots (in separate subroutine)
# Wrapper for R scripts which actually create plots

# This script:
# * extracts plate names and call rates from standard input
# * writes to file for R input
# * runs R scripts to create plot

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotShared; 
use WTSI::Genotyping::QC::QCPlotTests;

my ($mode, $RScriptPath, $outDir, $title, $help, $test);

GetOptions("mode=s"     => \$mode,
           "R=s"        => \$RScriptPath,
	   "out_dir=s"  => \$outDir,
	   "title=s"    => \$title,
	   "h|help"     => \$help);


if ($help) {
    print STDERR "Usage: $0 [ options ] 
Script to boxplots by plate for QC metrics.
Plots include call rate, autosome heterozygosity, and xy intensity difference.
Appropriate input data must be supplied to STDIN: either sample_cr_het.txt or the *XYdiff.txt file.

Options:
--mode=KEY          Keyword to determine plot type. Must be one of: cr, het, xydiff
--out_dir=PATH      Output directory for plots
--R=PATH            Path to Rscript installation to execute plots
--help              Print this help text and exit
Unspecified options will receive default values, with output written to: ./platePlots
";
    exit(0);
}

# mode is a string; one of cr, het, xydiff.
# mode determines some custom options (eg. xydiff scale), also used to construct filenames
# default options
$mode ||= "cr";
$RScriptPath ||=  $WTSI::Genotyping::QC::QCPlotShared::RScriptExec;
$outDir ||= 'testBoxPlots';
$title ||= "UNTITLED";
$test ||= 1; # test mode is on by default

my $scriptDir = $Bin."/".$WTSI::Genotyping::QC::QCPlotShared::RScriptsRelative; 

sub parsePlate {
    # parse plate from sample name, assuming usual PLATE_WELL_ID format
    my @terms = split(/_/, $_[0]);
    return $terms[0];
}

sub writeBoxplotInput {
    # read given input filehandle; write plate name and data to given output filehandle
    # data is taken from a particular index in space-separated input (eg. sample_cr_het.txt)
    my ($input, $output, $index) = @_;
    while (<$input>) {
	if (/^#/) { next; } # ignore comments
	chomp;
	my @words = split;
	my $plate = parsePlate($words[0]);
	print $output $plate."\t".$words[$index]."\n";
    }
}

sub runBeanPlot {
    # optionally, also generate a beanplot
    # may have to swap things around if beanplots become the default mode of operation!
    my ($mode, $RScriptPath, $outDir, $title, $scriptDir, $textPath, $test) = @_;
    my %plotScripts = ( # R plotting scripts for each mode
	cr     => $scriptDir.'beanplotCR.R',
	het    => $scriptDir.'beanplotHet.R', 
	xydiff => $scriptDir.'beanplotXYdiff.R', );
    my $pngOutPath = $outDir."/".$mode."_beanplot.png";
    my @args = ($RScriptPath, $plotScripts{$mode}, $textPath, $title);
    my @outputs = ($pngOutPath,);
    my $result = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs, $test);
    return $result;
}

sub run {
    # mode = cr, het or xydiff
    my ($mode, $RScriptPath, $scriptDir, $outDir, $title, $test) = @_;
    my %plotScripts = ( # R plotting scripts for each mode
	cr     => $scriptDir.'boxplotCR.R',
	het    => $scriptDir.'boxplotHet.R', 
	xydiff => $scriptDir.'boxplotXYdiff.R', );
    my %index = ( # index in whitespace-separated input data for each mode; use to write .txt input to R scripts
	cr     => 1,
	het    => 2, 
	xydiff => 1, );
    my $input = \*STDIN;
    my $textOutPath = $outDir."/".$mode."_boxplot.txt";
    my $pngOutPath = $outDir."/".$mode."_boxplot.png";
    open my $output, "> $textOutPath" || die "Cannot open output file: $!";
    writeBoxplotInput($input, $output, $index{$mode});
    close $output;
    my @args = ($RScriptPath, $plotScripts{$mode}, $textOutPath, $title);
    my @outputs = ($pngOutPath,);
    if ($mode eq 'cr') { push(@outputs, $outDir."/platePopulationSizes.png"); }
    my $plotsOK = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs, $test);
    my $result = runBeanPlot($mode, $RScriptPath, $outDir, $title, $scriptDir, $textOutPath, $test);
    if ($test && $result==0) { $plotsOK = 0; }
    return $plotsOK;
}

my $ok = run($mode, $RScriptPath, $scriptDir, $outDir, $title, $test);
if ($ok) { exit(0); }
else { exit(1); }
