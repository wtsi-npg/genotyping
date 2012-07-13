#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# Plot frequencies of causes for samples failing QC

# Possible failure causes: CR, Het, Gender, Duplicate, Identity, XYdiff
# Or any combination of the above!  This means 64 distinct combinations, ie. 2**[failure modes] 

# write .txt input for R plotting scripts, then execute scripts (using test wrapper)
# R scripts: plotCombinedFails.R, plotIndividualFails.R, scatterPlotFails.R

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use Carp qw(confess);
use WTSI::Genotyping::QC::QCPlotShared;
use WTSI::Genotyping::QC::QCPlotTests;

my ($configPath, $inPath,  $crHetPath, $outputDir, $help, $failText, $comboText, $causeText, 
    $crHetFail, $comboPng, $causePng, $scatterPng, $detailPng, $minCR, $maxHetSd, $title);

GetOptions("config=s"      => \$configPath,
           "input=s"       => \$inPath,
	   "cr-het=s"      => \$crHetPath,
	   "help"          => \$help,
	   "output-dir=s"  => \$outputDir,
	   "title=s",      => \$title,
    );

$inPath     ||= './qc_results.json';
$configPath ||= $Bin."/../json/qc_threshold_defaults.json";
$crHetPath  ||= './sample_cr_het.txt';
$failText   ||= 'failTotals.txt';
$comboText  ||= 'failCombos.txt';
$causeText  ||= 'failCauses.txt';
$crHetFail  ||= 'failedSampleCrHet.txt';
$comboPng   ||= 'failsCombined.png';
$causePng   ||= 'failsIndividual.png';
$scatterPng ||= 'failScatterPlot.png';
$detailPng  ||= 'failScatterDetail.png';
$outputDir  ||= '.';
$title      ||= 'Untitled';

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--input=PATH        Path to input file; defaults to ./qc_results.json
--output-dir=PATH   Path to output directory; defaults to current working directory
--title=STRING      Title for experiment to display in plots
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

my @outputPaths;
my @outNames = ($failText, $comboText, $causeText, $comboPng, $causePng, $crHetFail, $scatterPng, $detailPng);
if ($outputDir !~ /\/$/) { $outputDir .= '/'; }
foreach my $name (@outNames) { push(@outputPaths, $outputDir.$name); }

sub containsFailedSample {
    # check results hash for a failed sample
    my %qcResults = %{ shift() };
    my $fail = 0;
    foreach my $sample (keys(%qcResults)) {
	my %results = %{$qcResults{$sample}};
	foreach my $metric (keys(%results)) {
	    my ($pass, $value) = @{$results{$metric}};
	    if (int($pass) == 0) { $fail = 1; last; }
	}
	if ($fail) { last; }
    }
    return $fail;
}

sub findFailedCrHet {
    # find cr/het metrics for failed samples
    my ($allFailsRef, $namesRef, $crRef, $hetRef) = @_;
    my %failed;
    foreach my $name (@$allFailsRef) { $failed{$name} = 1; }
    my @names = @$namesRef;
    my @cr = @$crRef;
    my @het = @$hetRef;
    my %failedCrHet;
    for (my $i=0;$i<@names;$i++) {
	if ($failed{$names[$i]}) { 
	    $failedCrHet{$names[$i]} = [ ($cr[$i], $het[$i]) ];
	}
    }
    return %failedCrHet;
}

sub findHetMeanSd {
    # find mean and sd of autosome heterozygosity
    my $inPath = shift;
    my @data = WTSI::Genotyping::QC::QCPlotShared::readSampleData($inPath);
    my @hets;
    foreach my $fieldsRef (@data) {
	my @fields = @$fieldsRef;
	push(@hets, $fields[2]);
    }
    return WTSI::Genotyping::QC::QCPlotShared::meanSd(@hets);
}

sub sortFailCodes {
    # want to sort failure codes: all one-letter codes, then all two-letter, then all three-letter, etc.
    # groups "numbers of causes" together in plot
    my %codesByLen; # hash of arrays for each length
    my $max = 0;
    foreach my $code (@_) { 
	my $len = length($code);
	$codesByLen{$code} = $len;
	if ($len>$max) { $max = $len; }
    }
    my @sorted;
    for (my $i=1;$i<=$max;$i++) {	
	my @codes = ();
	foreach my $code (@_) { # repeated loop is inefficient, but doesn't matter with <= 32 codes!
	    if ($codesByLen{$code}==$i) { push(@codes, $code); }
	}
	push(@sorted, (sort(@codes)));
    }
    return @sorted;
}

sub writeFailCounts {
    # write counts of (individual and combined) failure causes
    # return array of failed sample names
    my ($qcResultsRef, $failText, $comboText) = @_;
    my %results = %$qcResultsRef;
    my (%singleFails, %combinedFails, @failedSamples);
    my %shortNames = WTSI::Genotyping::QC::QCPlotShared::readQCShortNameHash();
    foreach my $sample (keys(%results)) {
	my %metricResults = %{$results{$sample}};
	my @fails = ();
	foreach my $metric (keys(%metricResults)) {
	    my ($pass, $value) = @{$metricResults{$metric}};
	    if ($pass) { next; }
	    push(@fails, $shortNames{$metric} );
	    $singleFails{$metric}++;
	}
	my $combo = join('', sort(@fails));
	if ($combo ne '') { 
	    push(@failedSamples, $sample);
	    $combinedFails{$combo}++;
	};
    }
    open OUT, "> $failText" || die "Cannot open output file $failText: $!"; # individual failures
    my @metrics = sort(keys(%singleFails));
    foreach my $metric (@metrics) {
	print OUT $metric."\t".$singleFails{$metric}."\n";
    }
    close OUT;
    my @failCombos = sort(keys(%combinedFails));
    open OUT, "> $comboText" || die "Cannot open output file $failText: $!"; # combined failures
    foreach my $combo (@failCombos) {
	print OUT $combo."\t".$combinedFails{$combo}."\n";
    }
    close OUT;
    return @failedSamples;
}

sub writeFailedCrHet {
    # write cr, het, and pass/fail status by metric for failed samples
    my ($failedSamplesRef, $qcResultsRef, $crHetPath, $outPath) = @_;
    my %failedSamples;
    foreach my $sample (@$failedSamplesRef) { $failedSamples{$sample} = 1; }
    my %qcResults = %$qcResultsRef;
    my @data = WTSI::Genotyping::QC::QCPlotShared::readSampleData($crHetPath);
    open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
    my @header = qw(sample cr het);
    my @keys = qw(duplicate gender identity xydiff);
    push(@header, @keys);
    print OUT join("\t", @header)."\n";
    foreach my $fieldsRef (@data) {
	my @fields = splice(@$fieldsRef, 0, 3);
	my $sample = $fields[0];
	unless ($failedSamples{$sample}) { next; }
	# record duplicate, gender, identity, xydiff status
	my %qcResult = %{$qcResults{$sample}};
	foreach my $key (@keys) {
	    my $result = $qcResult{$key};
	    my $pass;
	    if ($result) { $pass = shift(@{$result}); } 
	    else { $pass = 1; } # may not have qc results for all (metric, sample) pairs
	    push(@fields, $pass);
	}
	print OUT join("\t", @fields)."\n";
    }
    close OUT;  
}

sub run {
    # find failure causes and write input for R scripts
    my ($inputPath, $qcConfigPath, $outputsRef, $title, $crHetPath) = @_;
    my %qcResults = WTSI::Genotyping::QC::QCPlotShared::readQCResultHash($inputPath);
    unless (containsFailedSample(\%qcResults)) {
	print STDERR "No samples failed QC thresholds; omitting failure plots.\n";
	return 1;
    }
    my ($failText, $comboText, $causeText, $comboPng, $causePng, $crHetFail, $scatterPng, $detailPng) 
	= @$outputsRef;
    my @failedSamples = writeFailCounts(\%qcResults, $failText, $comboText);
    my $failTotal = @failedSamples;
    writeFailedCrHet(\@failedSamples, \%qcResults, $crHetPath, $crHetFail);
    # run R scripts to produce plots
    # barplot individual failures
    my @args = ("plotIndividualFails.R", $failText, $failTotal, $title);
    my @outputs = ($causePng,);
    my $ok = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    unless ($ok) { confess "Error for individual failure barplot: $!"; }
    # barplot combined failures
    @args = ("plotCombinedFails.R", $comboText, $title);
    @outputs = ($comboPng,);
    $ok = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    unless ($ok) { confess "Error for combined failure barplot: $!"; }    
    my %thresholds =  WTSI::Genotyping::QC::QCPlotShared::readThresholds($qcConfigPath);
    my ($hetMean, $hetSd) = findHetMeanSd($crHetPath);
    my $hetMaxDist = $hetSd * $thresholds{'heterozygosity'};
    @args = ("scatterPlotFails.R", $crHetFail, $hetMean, $hetMaxDist, $thresholds{'call_rate'}, $title);
    @outputs = ($scatterPng, $detailPng);
    $ok = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    unless ($ok) { confess "Error for failure scatterplot: $!"; }
    return $ok;
}

my $allPlotsOK = run($inPath, $configPath, \@outputPaths, $title, $crHetPath);
if ($allPlotsOK) { exit(0); }
else { exit(1); }
