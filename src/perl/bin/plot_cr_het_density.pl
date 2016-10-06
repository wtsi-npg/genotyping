#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# replaces sample_cr_het.png
# create a heatmap of cr vs. het on a log scale; also do scatterplot & histograms of cr and het rate
# do plotting with R

use strict;
use warnings;
use Carp;
use Getopt::Long;
use FindBin qw($Bin);
use POSIX qw(floor);
use WTSI::NPG::Genotyping::QC::QCPlotTests;

our $VERSION = '';

my ($outDir, $title, $help);

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
        my ($xbin, $ybin);
        if ($xwidth==0) { $xbin = 0; }
        elsif ($x==$xmax) { $xbin = $xsteps - 1; }
        else { $xbin = floor(($x-$xmin)/$xwidth); }
        if ($ywidth==0) { $ybin = 0; }
        elsif ($y==$ymax) { $ybin = $ysteps - 1; }
        else { $ybin = floor(($y-$ymin)/$ywidth); }
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
	if (m{^\#}msx) { next; } # ignore comments
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
    return 1;
}

sub run {
    my $title = shift;
    my $outDir = shift;
    my @names = ('crHetDensityHeatmap.txt', 'crHetDensityHeatmap.pdf',
                 'crHetDensityHeatmap.png',
                 'crHet.txt',  'crHetDensityScatter.pdf',
                 'crHetDensityScatter.png', 'crHistogram.png',
                 'hetHistogram.png');
    my @paths = ();
    foreach my $name (@names) { push(@paths, $outDir.'/'.$name); }
    my ($output, @args, @outputs);
    my ($heatText, $heatPdf, $heatPng, $scatterText, $scatterPdf, $scatterPng, $crHist, $hetHist) = @paths;
    my $heatPlotScript = "heatmapCrHetDensity.R";
    ### read input and do heatmap plot ###
    my $input = \*STDIN;
    my ($coordsRef, $hetMin, $hetMax) = readCrHet($input);
    my ($xmin, $xmax, $xsteps, $ysteps) = (0, 41, 40, 40);
    my @counts = getBinCounts($coordsRef, $xmin, $xmax, $xsteps, $hetMin, $hetMax, $ysteps);
    open $output, ">", $heatText ||
        croak("Cannot open output path '", $heatText, "': $!");
    writeTable(\@counts, $output);
    close $output || croak("Cannot close output path '", $heatText, "'");;
    @args = ($heatPlotScript, $heatText, $title, $hetMin, $hetMax, $heatPdf);
    @outputs = ($heatPng,);
    my $plotsOK = WTSI::NPG::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    ### do scatterplot & histograms ###
    if ($plotsOK) {
        open $output, ">", $scatterText ||
            croak("Cannot open output path '", $scatterText, "': $!");
        writeTable($coordsRef, $output); # note that CR coordinates have been transformed to phred scale
	close $output ||
            croak("Cannot close output path '", $scatterText, "'");
	my $scatterPlotScript = "plotCrHetDensity.R";
	@args = ($scatterPlotScript, $scatterText, $title, $scatterPdf);
	@outputs = ($scatterPng, $crHist, $hetHist);
	$plotsOK = WTSI::NPG::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    }
    return $plotsOK;
}

my $ok = run($title, $outDir, $test);
if ($ok) { exit(0); }
else { exit(1); }


__END__

=head1 NAME

plot_cr_het_density

=head1 DESCRIPTION

Create density plots for genotype call rate and heterozygosity using R

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2013, 2014, 2015, 2016 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
