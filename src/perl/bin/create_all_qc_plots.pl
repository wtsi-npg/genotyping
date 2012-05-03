#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# 'main' script to generate plots for a given genotype analysis directory

use lib '/nfs/users/nfs_i/ib5/mygit/genotype_qc/qcPlots/'; # TODO change to production dir (or find dynamically?)
use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use QCPlotShared; # qcPlots module to define constants
use QCPlotTests;

my $start = time(); 
my ($tests, $failures) = (0,0);
my ($help, $scriptDir, $inputDir, $outDir, $refDir, $cmd, $test, $testLogPath, $testFH, $noSequenom);

sub getInputs {
    # find input files; check they are readable
    # warn if xydiff path missing; error if crhet path missing
    my $inputDir = shift;
    if ($inputDir !~ /\/$/) { $inputDir .= '/'; }
    my $crHetPath = $inputDir.$QCPlotShared::sampleCrHet;
    if (not -r $crHetPath) { die "ERROR: Cannot read sample cr/het path $crHetPath: $!"; }
    my @globResults = glob($inputDir.$QCPlotShared::xyDiffExpr); # input file of the form CHIPNAME_XYdiff.txt
    my $xyDiffPath = shift(@globResults);
    if (not(defined $xyDiffPath)) {
	print STDERR "WARNING: XYdiff path not found. (XYdiff calculation not applied to this analysis?)\n"; 
    } elsif (not -r $xyDiffPath) {
	print STDERR "WARNING: Cannot read XYdiff path: $xyDiffPath\n";
	$xyDiffPath = undef;
    }
    return ($crHetPath, $xyDiffPath)
}

sub getTitle {
    # get last two non-empty terms from path
    # truncate long parent directory names
    my $dir = shift;
    my $maxParentLen = 15;
    $dir =~ s/\/+/\//g; # get rid of redundant / characters
    my @terms = split(/\//, $dir);
    if ($terms[-1] eq '') { pop(@terms); }
    my $parent = $terms[-2];
    my $title;
    if ($parent) {
	my @chars = split('', $parent);
	if (@chars > $maxParentLen) { 
	    @chars = @chars[0..$maxParentLen]; 
	    $parent = join('', @chars).'...';
	}
	$title = $parent."/".$terms[-1];
    } elsif ($terms[0]) { # only one directory in path
	$title = $terms[0];
    } else {
	$title = "Unknown";
    }
    return $title;
}

######################################
### process command-line arguments ###
# IMPORTANT:  Cannot use ~ in arguments (Perl doesn't do shell expansion)
GetOptions("script_dir=s" => \$scriptDir,
	   "input_dir=s"  => \$inputDir,
           "output_dir=s" => \$outDir,
	   "test"         => \$test,
	   "test_log=s"   => \$testLogPath,
	   "ref_dir=s"    => \$refDir, 
	   "help"         => \$help,
	   "no_sequenom"  => \$noSequenom,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--input_dir=PATH    Location of input files; defaults to current working directory.
--output_dir=PATH   Location of output files; defaults to 'qcPlots' in input directory.
--ref_dir=PATH      Reference directory containing previous output for testing; only relevant in test mode.
--script_dir=PATH   Location of QC plotting scripts; defaults to location of main script.
--test              Test mode: Run tests on commands and output, and print results to standard output.  
--test_log=PATH     Write test results to given PATH. (Automatically enables --test mode.)
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

if ($testLogPath) { $test = 1; } # enable test mode if test log path supplied
$scriptDir ||= $Bin.'/';
$inputDir ||= '.';
$outDir ||= $inputDir.'/qcPlots';
if ($noSequenom) { $noSequenom = 1; }
else { $noSequenom = 0; }

########################################
### start execution of 'main' script ###

if ($test) {
    if ($testLogPath) { open $testFH, "> $testLogPath" || die "ERROR: Cannot open test log $testLogPath: $!"; }
    else { $testFH = *STDOUT; }
    print $testFH "$0:\tTest mode enabled.\n";
} else {
    $testFH = undef; # no test output
}
# sanity checks on input/output directories; if output directory doesn't exist, try to create it
unless (-r $inputDir) { die "ERROR: Cannot read input directory $inputDir: $!"; }
if ($test) {$tests++; print $testFH "OK\tFound input directory.\n";}
if (-e $outDir) { 
    if (not -w $outDir || not -d $outDir) { # output path not writable, or not a directory
	die "ERROR:  Cannot write to output directory $outDir: $!"; 
    }
} else {
    my $rc = system("mkdir -p $outDir"); 
    if ($rc!=0) { die "ERROR:  Cannot create output directory $outDir: $!"; } # non-zero return code
}
if ($test) {$tests++; print $testFH "OK\tFound output directory.\n";}

my $title = getTitle($inputDir); # title shared between several plots, eg. MY_EXPERIMENT/illuminus1
my ($crHetPath, $xyDiffPath) = getInputs($inputDir);

my $doHeatPlot = 1; # switch to enable/disable generation of plate heatmap plots

##########################
### plate heatmap plots ##
if ($doHeatPlot) {
    my $heatMapScript = $scriptDir.'/plate_heatmap_plots.pl';
    my $hmOut = $outDir.'/'.$QCPlotShared::plateHeatmapDir; # output heatmaps to subdirectory of main output 
    unless (-e $hmOut) { mkdir($hmOut) || die "Cannot create heatmap directory $hmOut: $!" }
    if ($test) { $tests++; print $testFH "OK\tFound plate heatmap subdirectory.\n"; }
    foreach my $mode ('cr', 'het') { # assumes $crHetPath exists and is readable
	$cmd = join(' ', ('cat', $crHetPath, '|', 'perl', $heatMapScript, '--mode='.$mode, '--out_dir='.$hmOut));
	($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);
    }
    if ($xyDiffPath) { # repeat for xydiff input (if any)
	$cmd = join(' ', ('cat', $xyDiffPath, '|', 'perl', $heatMapScript, '--mode=xydiff', '--out_dir='.$hmOut));
	($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);
    }
    my $indexScript = $scriptDir.'/plate_heatmap_index.pl';
    my $indexName = $QCPlotShared::plateHeatmapIndex;
    $cmd = "perl $indexScript $title $hmOut $indexName";
    ($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);
}

################
### boxplots ###
my $boxPlotScript = $scriptDir.'/plot_box_bean.pl';
my @modes = ('cr', 'het', 'xydiff');
my @inputs = ($crHetPath, $crHetPath, $xyDiffPath);
for (my $i=0; $i<@modes; $i++) {
    unless (defined($inputs[$i]) && -r $inputs[$i]) { next; } # skip undefined inputs; xydiffpath may not exist
    $cmd = join(' ', ('cat', $inputs[$i], '|', 'perl', $boxPlotScript, '--mode='.$modes[$i], 
		      '--out_dir='.$outDir, '--title='.$title));
    ($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);
}

######################################################################
### global cr/het density plots: heatmap, scatterplot & histograms ###
my $globalCrHetScript = $scriptDir.'/plot_cr_het_density.pl';
my $prefix = $outDir.'/crHetDensity';
$cmd = join(' ', ('cat', $crHetPath, '|', 'perl', $globalCrHetScript, "--title=".$title, "--out_dir=".$outDir));
($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);

################################
### failure cause breakdowns ###
my $failPlotScript = $scriptDir."/plot_fail_causes.pl";
$cmd = join(' ', ('perl', $failPlotScript, "--input_dir=".$inputDir, "--output_dir=".$outDir, "--title=".$title));
if ($noSequenom) { $cmd .= ' --no_sequenom'; }
($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);

################################
### html index for all plots ###
my $plotIndexScript = $scriptDir.'/main_plot_index.pl';
$cmd = join(' ', ('perl', $plotIndexScript, $title, $outDir));
($tests, $failures) = QCPlotTests::wrapCommand($cmd, $testFH, $tests, $failures);

#################################################################
### check intermediate text output against reference (if any) ###
if ($test && $refDir) {
    ($tests, $failures) = QCPlotTests::diffGlobs($refDir, $outDir, $testFH, $tests, $failures);
    if ($doHeatPlot) {
	my $hm = $QCPlotShared::plateHeatmapDir;
	($tests, $failures) = QCPlotTests::diffGlobs($refDir.'/'.$hm, $outDir.'/'.$hm, $testFH, $tests, $failures);
    }
}

########################################
### output summary of tests (if any) ###
my $duration = time() - $start;
if ($test) { 
    print $testFH "Finished.\nTotal tests:\t$tests\nTotal failures:\t$failures\nDuration:\t$duration s\n"; 
}
if ($testLogPath) { close $testFH; }
