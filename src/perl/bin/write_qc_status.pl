#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# June 2012

# identify genotyping QC failures 
# record metric values and pass/fail status in a JSON format file
# use this file to produce input for plot scripts, and human-readable reports

# evaluate pass/fail for given metrics & thresholds
# input thresholds from JSON config file; may be default or user-specified
# threshold may be "NA" (eg. for gender and identity checks, which are a "black box" pass/fail)
# write result data structure to JSON output file

# typical metrics: CR, het, duplicate, identity, gender, xydiff

use strict;
use warnings;
use FindBin qw($Bin);
use Getopt::Long;
use JSON;
use WTSI::Genotyping::QC::QCPlotShared;

my ($inputDir, $outputDir, $configPath, $help);

GetOptions("input-dir=s"   => \$inputDir,
	   "output-dir=s"  => \$outputDir,
	   "config=s"      => \$configPath,
	   "help"          => \$help,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--input-dir         Directory containing input files (defaults to current working directory)
--output-dir        Output directory (defaults to input directory)
--config            Config path in JSON format, specifying metrics and thresholds (defaults to standard file)
--help              Print this help text and exit
";
    exit(0);
}

$inputDir ||= '.';
$outputDir ||= $inputDir;
$configPath ||= $Bin."/../json/qc_threshold_defaults.json";
my %fileNames = WTSI::Genotyping::QC::QCPlotShared::readQCFileNames();
my $outPath = $outputDir.'/'.$fileNames{'qc_results'};

run($inputDir, $configPath, $outPath);

sub convertResults {
    # convert results from metric-major to sample-major ordering
    my %metricResults = %{ shift() };
    my %sampleResults = ();
    foreach my $metric (keys(%metricResults)) {
	my %resultsBySample = %{$metricResults{$metric}};
	foreach my $sample (keys(%resultsBySample)) {
	    my $resultRef = $resultsBySample{$sample};
	    $sampleResults{$sample}{$metric} = $resultRef;
	}	
    }
    return %sampleResults;
}

sub findMetricResults {
    # find input path(s) and QC results for given metric
    my ($inputDir, $metric, $threshold, $input) = @_;
    my %results;
    if ($metric eq 'call_rate') { 
	%results = resultsCr($threshold, $inputDir.'/'.$input);
    } elsif ($metric eq 'heterozygosity') { 
	%results = resultsHet($threshold, $inputDir.'/'.$input);
    } elsif ($metric eq 'duplicate') { 
	my @input = @$input;
	my @paths = ();
	foreach my $name (@input) { push(@paths, $inputDir."/".$name); } 
	%results = resultsDuplicate($threshold, \@paths);	    
    } elsif ($metric eq 'identity') { 
	%results = resultsIdentity($threshold, $inputDir.'/'.$input);
    } elsif ($metric eq 'gender') { 
	%results = resultsGender($threshold, $inputDir.'/'.$input);
    } elsif ($metric eq 'xydiff') { 
	%results = resultsXydiff($threshold, $inputDir.'/'.$input);
    } else { die "Unknown QC metric $metric: $!"; }
    return %results;
}

sub readSampleNames {
    # read sample names from sample_cr_het.txt (or any other tab-delimited file with sample name as first field)
    # Now obsolete for this script! But may be useful to preserve sample order in human-readable output.
    my $inPath = shift;
    open IN, "< $inPath" || die "Cannot open input path $inPath: $!";
    my $i = 0;
    my @samples = ();
    while (<IN>) {
	if (/^#/) { next; } # comments start with a #
	chomp;
	my @words = split;
	push(@samples, shift(@words));
    }
    close IN;
    return @samples;
}


sub resultsCr {
    # find call rate (CR) and pass/fail status of each sample
    my ($threshold, $inPath) = @_;
    my @data =  WTSI::Genotyping::QC::QCPlotShared::readSampleData($inPath);
    my %results;
    foreach my $ref (@data) {
	my @fields = @$ref;
	my ($sample, $cr) = ($fields[0], $fields[1]);
	my $pass;
	if ($cr >= $threshold) { $pass = 1; }
	else { $pass = 0; }
	$results{$sample} = [$pass, $cr];
    }
    return %results;
}

sub resultsDuplicate {
    # find duplicate pairs from duplicate_summary.txt
    # exclude one member of each pair (the one with lower call rate; in a tie, the first "alphabetically")
    # read call rates from sample_cr_het.txt
    # $threshold is not used
    my ($threshold, $inPathsRef) = @_;
    my ($crHetPath, $duplicatePath, ) = @$inPathsRef;
    my (%duplicates, @pairs, %results);
    # read duplicate sample names
    open IN, "< $duplicatePath" || die "Cannot open input path $duplicatePath: $!";
    while (<IN>) {
	if (/^#/) { next; } # comments start with a #
	my @fields = split;
	my @pair = ($fields[1], $fields[2]);
	foreach my $sample (@pair) { $duplicates{$sample} = 1; }
	push(@pairs, \@pair);
    }
    close IN;
    # read call rates for duplicated samples
    my (%duplicateCR, @samples);
    my @data = WTSI::Genotyping::QC::QCPlotShared::readSampleData($crHetPath);
    foreach my $ref (@data) {
	my @fields = @$ref;
	my ($sample, $cr) = ($fields[0], $fields[1]);
	if ($duplicates{$sample}) { $duplicateCR{$sample} = $cr; }
	push(@samples, $sample);
    }
    # choose which duplicates to keep/discard
    my %pass;
    foreach my $pairRef (@pairs) {
	my ($sam1, $sam2) = @$pairRef;
	if ($duplicateCR{$sam1} > $duplicateCR{$sam2}) { $pass{$sam1} = 1; $pass{$sam2} = 0; }
	elsif ($duplicateCR{$sam1} < $duplicateCR{$sam2}) { $pass{$sam1} = 0; $pass{$sam2} = 1; } 
	elsif ($sam1 lt $sam2) { $pass{$sam1} = 1; $pass{$sam2} = 0; }
	else { $pass{$sam1} = 0; $pass{$sam2} = 1; } 
    }
    # fill in results for all samples; use 0/1 for not_duplicate/duplicate metric values
    foreach my $sample (@samples) {
	if (!$duplicates{$sample}) { $results{$sample}=[1,0]; }
	elsif ($pass{$sample}) { $results{$sample} = [1,1]; }
	else { $results{$sample}=[0, 1]; }
    }
    return %results;
}

sub resultsGender {
    # read gender results from sample_xhet_gender.txt
    # 'metric value' is concatenation of inferred, supplied gender codes
    # $threshold not used
    my ($threshold, $inPath) = @_;
    my @data =  WTSI::Genotyping::QC::QCPlotShared::readSampleData($inPath, 1); # skip header on line 0
    my %results;
    foreach my $ref (@data) {
	my @fields = @$ref;
	my ($sample, $inferred, $supplied) = ($fields[0], $fields[2], $fields[3]);
	my $pass;
	if ($inferred==$supplied) { $pass = 1; }
	else { $pass = 0; }
	$results{$sample} = [$pass, $inferred.$supplied];
    }
    return %results;
}

sub resultsHet {
    # find autosome het rate and pass/fail status of each sample
    my ($threshold, $inPath) = @_;
    my $index = 2;
    return resultsMetricSd($threshold, $index, $inPath);
}

sub resultsIdentity {
    # read results of concordance check with sequenom results
    my ($threshold, $inPath) = @_;
    my @data =  WTSI::Genotyping::QC::QCPlotShared::readSampleData($inPath);
    my %results;
    foreach my $ref (@data) {
	my @fields = @$ref;
	my ($sample, $concord) = ($fields[0], $fields[3]);
	my $pass;
	if ($concord >= $threshold) { $pass = 1; }
	else { $pass = 0; }
	if ($concord == 1) { $concord = "1.0"; } # removes unwanted trailing zeroes
	$results{$sample} = [$pass, $concord];
    }
    return %results;
}

sub resultsMetricSd {
    # find results for given field, fail samples too far from the mean; use for het rate, xydiff
    # threshold expressed in standard deviations; first need to find absolute thresholds
    my ($threshold, $index, $inPath, $startLine) = @_;
    my (@samples, @values, $pass, %results);
    my @data =  WTSI::Genotyping::QC::QCPlotShared::readSampleData($inPath, $startLine);
    foreach my $ref (@data) {
	my @fields = @$ref;
	push(@samples, $fields[0]);
	push(@values, $fields[$index]);
    }
    my ($mean, $sd) = WTSI::Genotyping::QC::QCPlotShared::meanSd(@values);
    my $min = $mean - $threshold*$sd;
    my $max = $mean + $threshold*$sd;
    for (my $i=0;$i<@samples;$i++) {
	if ($values[$i] >= $min && $values[$i] <= $max) { $pass = 1; }
	else { $pass = 0; }
	$results{$samples[$i]} = [$pass, $values[$i]];
	#if ($i % 100 == 0) { print join("\t", $i, $pass, $values[$i])."\n"; } ### test
    }
    return %results;
}

sub resultsXydiff {
    my ($threshold, $inPath) = @_;
    my $index = 1;
    my $startLine = 1;
    return resultsMetricSd($threshold, $index, $inPath, $startLine);
}

sub writeResults {
    # find QC results and write to given filehandle in JSON format
    # assumes all results will fit in memory!  If not, will need to rethink I/O and file format.
    my ($out, $sep, $inputDir, $metricsRef, $thresholdsRef, $inputNamesRef) = @_;
    my @metrics = @$metricsRef;
    my %thresholds = %$thresholdsRef;
    my %inputNames = %$inputNamesRef;
    my %metricResults;
    foreach my $metric (@metrics) {
	my %results = findMetricResults($inputDir, $metric, $thresholds{$metric}, $inputNames{$metric});
	$metricResults{$metric} = \%results;
    }
    # change from metric-major to sample-major ordering; simplifies later data processing
    my %sampleResults = convertResults(\%metricResults);
    my $resultString = encode_json(\%sampleResults);
    print $out $resultString;
}

sub run {
    # main method to run script
    my ($inputDir, $configPath, $outPath, $sep) = @_;
    $sep ||= "\t";
    my %thresholds = WTSI::Genotyping::QC::QCPlotShared::readThresholds($configPath);
    my @metricNames = WTSI::Genotyping::QC::QCPlotShared::readQCNameArray();
    my @metrics = ();
    foreach my $metric (@metricNames) { 
	# use metrics with defined thresholds
	if (defined($thresholds{$metric})) { push(@metrics, $metric); }
    } 
    my %inputNames = WTSI::Genotyping::QC::QCPlotShared::readQCMetricInputs();
    my $out;
    open($out, "> $outPath") || die "Cannot open output path $outPath: $!"; 
    writeResults($out, $sep, $inputDir, \@metrics, \%thresholds, \%inputNames);
    close($out);
}


