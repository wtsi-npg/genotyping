#! /software/bin/perl

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
use Carp;
use Getopt::Long;
use FindBin qw($Bin);
use Log::Log4perl qw(:easy);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(getPlateLocationsFromPath);
use WTSI::NPG::Genotyping::QC::QCPlotTests;

our $VERSION = '';

Log::Log4perl->easy_init($ERROR);
my $log = Log::Log4perl->get_logger();

my ($mode, $type, $outDir, $title, $help, $dbpath, $inipath);

GetOptions("mode=s"     => \$mode,
	   "type=s"     => \$type,
	   "out_dir=s"  => \$outDir,
	   "title=s"    => \$title,
	   "dbpath=s"   => \$dbpath,
	   "inipath=s"  => \$inipath,
	   "h|help"     => \$help);


if ($help) {
    print STDERR "Usage: $0 [ options ]
Script to boxplots by plate for QC metrics.
Plots include call rate, autosome heterozygosity, and xy intensity difference.
Appropriate input data must be supplied to STDIN: either sample_cr_het.txt or the *XYdiff.txt file.

Options:
--mode=KEY          Keyword to determine plot type. Must be one of: cr, het, xydiff
--type=KEY          Keyword for plot type.  Must be one of: box, bean, both
--dbpath=PATH       Path to pipeline database containing plate information
--inipath=PATH      Path to .ini file for pipeline database
--out_dir=PATH      Output directory for plots
--help              Print this help text and exit
Unspecified options will receive default values, with output written to: ./platePlots
";
    exit(0);
}

# mode is a string; one of cr, het, xydiff.
# mode determines some custom options (eg. xydiff scale), also used to construct filenames
# default options
$mode ||= "cr";
$type = 'both';
$outDir ||= '.';
$title ||= "UNTITLED";

unless ($mode eq "cr" || $mode eq "het" || $mode eq "xydiff") {
  $log->logcroak("Illegal mode argument: ", $mode, "': $!");
}
unless ($type eq "box" || $type eq "bean" || $type eq "both") {
  $log->logcroak("Illegal type argument: '", $type, "': $!");
}
if ((!$dbpath) && (!$inipath)) {
  $log->logcroak("Must supply at least one of pipeline database path and .ini path");
}
if ($dbpath && !(-r $dbpath)) {
  $log->logcroak("Cannot read pipeline database path '", $dbpath, "': $!");
}
if ($inipath && !(-r $inipath)) {
  $log->logcroak("Cannot read .ini path '", $inipath, "'");
}

sub writeBoxplotInput {
    # read given input filehandle; write plate name and data to given output filehandle
    # data is taken from a particular index in space-separated input (eg. sample_cr_het.txt)
    my ($input, $output, $index, $dbpath,  $inipath) = @_;
    my $inputOK = 0;
    my %plateLocs = getPlateLocationsFromPath($dbpath, $inipath);
    while (<$input>) {
	if (m{^\#}msx) { next; } # ignore comments
	chomp;
	my @words = split;
	unless ($plateLocs{$words[0]}) {
            $log->logcroak("No plate location for sample '", $words[0],
                           "'; exiting");
    }
	my ($plate, $addressLabel) = @{$plateLocs{$words[0]}};
	if ($plate) {
	    $inputOK = 1; # require at least one sample with a valid plate!
	    print $output $plate."\t".$words[$index]."\n";
	}
    }
    return $inputOK;
}

sub runPlotScript {
    # run R script to create box/beanplot
    my ($mode, $bean, $outDir, $title, $textPath) = @_;
    my %beanPlotScripts = ( # R plotting scripts for each mode
	cr     => 'beanplotCR.R',
	het    => 'beanplotHet.R',
	xydiff => 'beanplotXYdiff.R', );
    my %boxPlotScripts = ( # R plotting scripts for each mode
	cr     => 'boxplotCR.R',
	het    => 'boxplotHet.R', 
	xydiff => 'boxplotXYdiff.R', );
    my ($plotScript, $pngOutPath);
    if ($bean) {
	$plotScript = $beanPlotScripts{$mode};
	$pngOutPath = $outDir."/".$mode."_beanplot.png";
    }
    else {
	$plotScript = $boxPlotScripts{$mode};
	$pngOutPath = $outDir."/".$mode."_boxplot.png";
    }
    my @args = ($plotScript, $textPath, $title);
    my @outputs = ($pngOutPath,);
    if (!$bean && $mode eq 'cr') { push(@outputs, $outDir.'/total_samples_per_plate.png'); }
    my ($ok, $cmd, $info) = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs, 1);
    if (not $ok) {
	# R script error should not be fatal; beanplot may fail on 'sparse' data
	my $msg = "Warning: R script failure: Command \"$cmd\", output \"$info\"\n";
	$log->error($msg);
    }
    return 1;
}

sub run {
    # mode = cr, het or xydiff
    # type = box, bean, or both
    my ($mode, $type, $outDir, $title, $dbpath, $inipath) = @_;
    my %index = ( # index in whitespace-separated input data for each mode; use to write .txt input to R scripts
	cr     => 1,
	het    => 2,
	xydiff => 1, );
    my $input = \*STDIN;
    my $textOutPath = $outDir."/".$mode."_boxplot.txt";
    open my $output, ">", $textOutPath ||
        $log->logcroak("Cannot open output file '", $textOutPath, "': $!");
    my $inputOK = writeBoxplotInput($input, $output, $index{$mode}, $dbpath, $inipath);
    close $output || $log->logcroak("Cannot close output file '",
                                    $textOutPath, "'");;
    my $plotsOK = 0;
    if ($inputOK) {
	if ($type eq 'both' || $type eq 'box') {
	    $plotsOK = runPlotScript($mode, 0,  $outDir, $title, $textOutPath); # boxplot
	}
	if ($plotsOK && ($type eq 'both' || $type eq 'bean')) {
	    $plotsOK = runPlotScript($mode, 1,  $outDir, $title, $textOutPath); # beanplot
	}
    } else {
	$log->logcroak("Cannot parse any plate names from standard input");
    }
    return $plotsOK;
}

my $ok = run($mode, $type, $outDir, $title, $dbpath, $inipath);
if ($ok) { exit(0); }
else { exit(1); }


__END__

=head1 NAME

plot_box_bean

=head1 DESCRIPTION

Generate boxplots and beanplots of QC metrics using R

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2013, 2015, 2016 Genome Research Limited.
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
