#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# replaces sample_cr_het.png
# create a heatmap of cr vs. het on a log scale; also do scatterplot & histograms of cr and het rate
# do plotting with R

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotTests;

my ($RScriptPath, $outDir, $title, $help);

GetOptions("out_dir=s"  => \$outDir,
	   "title=s"    => \$title,
	   "h|help"     => \$help);

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--out_dir=PATH      Output directory for plots
--title=TITLE       Title for experiment
--help              Print this help text and exit
Unspecified options will receive default values, with output written to current directory.
";
    exit(0);
}

$outDir ||= '.';
$title ||= 'UNTITLED';
my $test = 1;

sub getBinCounts {
    # input: array of (x,y) pairs; range and number of bins for x and y
    # output: array of arrays of counts
    my ($dataRef, $xmin, $xmax, $xsteps, $ymin, $ymax, $ysteps) = @_;
    my $xwidth = ($xmax - $xmin) / $xsteps;
    my $ywidth = ($ymax - $ymin) / $ysteps;
    my @data = @$dataRef;
    my @counts = ();
    for (my $i=0;$i<$xsteps;$i++) { 
	my @row = (0) x $ysteps;
	$counts[$i] = \@row;
    }
    foreach my $ref (@data) {
	my ($x, $y) = @$ref;
	# truncate x,y values outside given range (if any)
	if ($x > $xmax) { $x = $xmax; } 
	elsif ($x < $xmin) { $x = $xmin; }
	if ($y > $ymax) { $y = $ymax; }
	elsif ($y < $ymin) { $y = $ymin; }
	# find bin coordinates and increment count
	my $xbin = int(($x-$xmin)/$xwidth);
	my $ybin = int(($y-$ymin)/$ywidth);
	if ($y == $ymax) { $ybin -= 1; }
	$counts[$xbin][$ybin] += 1;
    }
    return @counts;
}

sub readCrHet {
    # read (cr, het) coordinates from given input filehandle
    # also get min/max heterozygosity
    my $input = shift;
    my $qMax = shift;
    $qMax ||= 40;
    my $crMax = 1 - 10**(-$qMax/10); # truncate very high CR (may have CR=100% for few SNPs)
    my ($crIndex, $hetIndex) = (1,2); 
    my @coords = ();
    my ($hetMin, $hetMax) = (1, 0);
    while (<$input>) {
	if (/^#/) { next; } # ignore comments
	chomp;
	my @words = split;
	my $cr = $words[$crIndex];
	my $crScore; # convert cr to phred scale
	if ($cr > $crMax) { $crScore = $qMax; } 
	else { $crScore = -10 * (log(1 - $words[$crIndex]) / log(10)); } 
	my $het = $words[$hetIndex];
	if ($het < $hetMin) { $hetMin = $het; }
	if ($het > $hetMax) { $hetMax = $het; }
	push(@coords, [$crScore, $het]);
    }
    return (\@coords, $hetMin, $hetMax);
}

sub writeTable {
    # write array of arrays to given filehandle
    my ($tableRef, $output) = @_;
    foreach my $rowRef (@$tableRef) {
	my @row = @$rowRef;
	print $output join("\t", @row)."\n";
    }
}

sub run {
    my $title = shift;
    my $outDir = shift;
    my $test = shift;
    my @names = ('crHetDensityHeatmap.txt', 'crHetDensityHeatmap.png', 'crHet.txt', 
		 'crHetDensityScatter.png', 'crHistogram.png', 'hetHistogram.png');
    my @paths = ();
    foreach my $name (@names) { push(@paths, $outDir.'/'.$name); }
    my ($cmd, $output, @args, @outputs, $result);
    my ($heatText, $heatPng, $scatterText, $scatterPng, $crHist, $hetHist) = @paths;
    my $heatPlotScript = "heatmapCrHetDensity.R";
    ### read input and do heatmap plot ###
    my $input = \*STDIN;
    my ($coordsRef, $hetMin, $hetMax) = readCrHet($input);
    my ($xmin, $xmax, $xsteps, $ysteps) = (0, 41, 40, 40);
    my @counts = getBinCounts($coordsRef, $xmin, $xmax, $xsteps, $hetMin, $hetMax, $ysteps);
    open $output, "> $heatText" || die "Cannot open output path $heatText: $!";
    writeTable(\@counts, $output);
    close $output;
    @args = ($heatPlotScript, $heatText, $title, $hetMin, $hetMax);
    @outputs = ($heatPng,);
    my $plotsOK = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs, $test);
    ### do scatterplot & histograms ###
    if ($plotsOK) {
	open $output, "> $scatterText" || die "Cannot open output path $scatterText: $!";
	writeTable($coordsRef, $output); # note that CR coordinates have been transformed to phred scale
	close $output;
	my $scatterPlotScript = "plotCrHetDensity.R";
	@args = ($scatterPlotScript, $scatterText, $title);
	@outputs = ($scatterPng, $crHist, $hetHist);
	$plotsOK = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs, $test);
    }
    return $plotsOK;    
}

my $ok = run($title, $outDir, $test);
if ($ok) { exit(0); }
else { exit(1); }
