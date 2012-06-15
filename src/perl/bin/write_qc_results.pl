#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# June 2012

# identify genotyping QC failures and record in a text format (tab or comma delimited)
# consider metrics; threshold may be "NA" (eg. for gender and identity checks, which are a "black box" pass/fail)
# read inputs, compare to threshold (if any), compute pass/fail
# header: QC metrics and thresholds used
# body: sample name, pass/fail, metric values; record as (pass/fail, value) pairs?

# typical metrics: CR, het, duplicate, identity, gender, xydiff (optional?)
# 1. read data from input paths (text files written by previous qc)
# 2. apply checks (with appropriate thresholds, if any)
# 3. write generic output
# 4. write specialist output for R scripts

use strict;
use warnings;
use Getopt::Long;
use WTSI::Genotyping::QC::QCPlotShared;

my ($inputDir, $outputDir, $help);

GetOptions("input-dir=s"   => \$inputDir,
	   "output-dir=s"  => \$outputDir,
	   "help"          => \$help,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

$inputDir ||= '.';
$outputDir ||= $inputDir;
my $outPath = $outputDir.'/qc_results.txt';

my $null = "NA";
#my $null = 0;
my @metrics = ('CR', 'Het', 'Duplicate', 'Identity', 'Gender');
my @thresholds = ($WTSI::Genotyping::QC::QCPlotShared::minCR, 
		  $WTSI::Genotyping::QC::QCPlotShared::maxHetSd, 
		  $null,
		  $WTSI::Genotyping::QC::QCPlotShared::minIdentity,
		  $null, 
    );

#my @metrics = ('CR', 'Het', 'Duplicate', 'Identity', 'Gender', 'Xydiff');
#my @thresholds = ($WTSI::Genotyping::QC::QCPlotShared::minCR,
#		  $WTSI::Genotyping::QC::QCPlotShared::maxHetSd,
#		  $null,
#		  $WTSI::Genotyping::QC::QCPlotShared::minIdentity,
#		  $null,
#		  $WTSI::Genotyping::QC::QCPlotShared::maxXydiffSd,
 #   );
my $sampleCrHet = $WTSI::Genotyping::QC::QCPlotShared::sampleCrHet;
my $duplicates = 'duplicate_summary.txt';
my $idents = 'identity_check_results.txt'; 
my $genders = 'sample_xhet_gender.txt';
my $xydiff = $WTSI::Genotyping::QC::QCPlotShared::xydiff;
my %inputNames = (CH   => $sampleCrHet,
		  D    => $duplicates,
		  I    => $idents,
		  G    => $genders,
		  XY   => $xydiff,
    );

run($inputDir, $outPath, \@metrics, \@thresholds, \%inputNames);


sub findMetricResults {
    # find input path(s) and QC results for given metric
    my ($inputDir, $metric, $threshold, $namesRef) = @_;
    my %names = %$namesRef; # names of input files
    my %results;
    if ($metric eq 'CR') { 
	%results = resultsCr($threshold, $inputDir.'/'.$names{'CH'});
    } elsif ($metric eq 'Het') { 
	%results = resultsHet($threshold, $inputDir.'/'.$names{'CH'});
    } elsif ($metric eq 'Duplicate') { 
	my @paths = ($inputDir.'/'.$names{'D'}, $inputDir.'/'.$names{'CH'});
	%results = resultsDuplicate($threshold, \@paths);	    
    } elsif ($metric eq 'Identity') { 
	%results = resultsIdentity($threshold, $inputDir.'/'.$names{'I'});
    } elsif ($metric eq 'Gender') { 
	%results = resultsGender($threshold, $inputDir.'/'.$names{'G'});
    } elsif ($metric eq 'Xydiff') { 
	%results = resultsXydiff($threshold, $inputDir.'/'.$names{'XY'});
    } else { die "Unknown QC metric $metric: $!"; }
    return %results;
}

sub meanSd {
    # find mean and standard deviation of input list
    # first pass -- mean
    my $total = 0;
    foreach my $x (@_) { $total+= $x; }
    my $mean = $total / @_;
    # second pass -- sd
    $total = 0;
    foreach my $x (@_) { $total += abs($x - $mean); }
    my $sd = $total / @_;
    return ($mean, $sd);
}

sub readSampleNames {
    # read sample names from sample_cr_het.txt (or any other tab-delimited file with sample name as first field)
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

sub readSampleData {
    # read data for given sample names from space-delimited file; return array of arrays of data read
    my ($inPath, $startLine, $samplesRef) = @_;
    $startLine ||= 0;
    $samplesRef ||= 0;
    my (%samples, $sampleTotal); # do we select a subset of samples?
    if ($samplesRef) {
	my @samples = @$samplesRef;
	$sampleTotal = @samples;
	foreach my $sample (@samples) { $samples{$sample} = 1; }
    }
    my @data;
    open IN, "< $inPath" || die "Cannot open input path $inPath: $!";
    my $line = 0;
    while (<IN>) {
	$line++;
	if (/^#/ || $line <= $startLine) { next; } # comments start with a #
	my @fields = split;
	if ($samplesRef && !$samples{$fields[0]}) { next; } # skip samples not in list
	push(@data, \@fields);
	if ($samplesRef && @data==$sampleTotal) { last; } # all target samples read
    }
    close IN;
    return @data;    
}

sub resultsCr {
    # find call rate (CR) and pass/fail status of each sample
    my ($threshold, $inPath) = @_;
    my @data = readSampleData($inPath);
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
    my ($duplicatePath, $crHetPath) = @$inPathsRef;
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
    my @data = readSampleData($crHetPath);
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
    my @data = readSampleData($inPath, 1); # skip header on line 0
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
    # threshold expressed in standard deviations; first need to find absolute thresholds
    my ($threshold, $inPath) = @_;
    my (@samples, @hets, $pass, %results);
    my @data = readSampleData($inPath);
    foreach my $ref (@data) {
	my @fields = @$ref;
	push(@samples, $fields[0]);
	push(@hets, $fields[2]);
    }
    my ($mean, $sd) = meanSd(@hets);
    my $min = $mean - $threshold*$sd;
    my $max = $mean + $threshold*$sd;
    for (my $i=0;$i<@samples;$i++) {
	if ($hets[$i] >= $min && $hets[$i] <= $max) { $pass = 1; }
	else { $pass = 0; }
	$results{$samples[$i]} = [$pass, $hets[$i]];
    }
    return %results;
}

sub resultsIdentity {
    # read results of concordance check with sequenom results
    my ($threshold, $inPath) = @_;
    my @data = readSampleData($inPath);
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

sub resultsXydiff {

}

sub writeHeader {
    # write qc header to given filehandle
    my ($out, $sep, $metricsRef, $thresholdsRef) = @_;
    my @metrics = @$metricsRef;
    my @thresholds = @$thresholdsRef;
    if ($#metrics != $#thresholds) { die "Metric and threshold lists of unequal length: $!"; }
    my $total = @thresholds;
    my @header = ($total, ); # first field is total number of metrics
    foreach my $metric (@metrics) { push(@header, $metric); }
    foreach my $threshold (@thresholds) { push(@header, $threshold); }
    print $out join($sep, @header)."\n";
}

sub writeResults {
    # find QC results and write to given filehandle
    # (TODO? may need to split samples into chunks and write each chunk individually, for larger projects)
    # results are pass/fail status for each metric, followed by metric values (if any) in same order
    my ($out, $sep, $inputDir, $metricsRef, $thresholdsRef, $inputNamesRef, $samplesRef) = @_;
    my @metrics = @$metricsRef;
    my @thresholds = @$thresholdsRef;
    my @samples = @$samplesRef;
    my @allResults;
    for (my $i=0;$i<@metrics;$i++) {
	my %results = findMetricResults($inputDir, $metrics[$i], $thresholds[$i], $inputNamesRef);
	push(@allResults, \%results);
    }
    foreach my $sample (@samples) {
	# generate output line for each sample and write to filehandle
	my @output = ($sample, );
	for (my $i=0;$i<@metrics;$i++) { # pass/fail status
	    push(@output, $allResults[$i]{$sample}[0]);
	}
	for (my $i=0;$i<@metrics;$i++) { # metric value
	    push(@output, $allResults[$i]{$sample}[1]);
	}
	print $out join($sep, @output)."\n";
    }
}

sub run {
    # main method to run script
    my ($inputDir, $outPath, $metricsRef, $thresholdsRef, $inputNamesRef, $sep) = @_;
    $sep ||= "\t";
    my %inputNames = %$inputNamesRef;
    my @samples = readSampleNames($inputDir."/".$inputNames{'CH'});
    my $out;
    open($out, "> $outPath") || die "Cannot open output path $outPath: $!"; 
    writeHeader($out, $sep, $metricsRef, $thresholdsRef);
    writeResults($out, $sep, $inputDir, $metricsRef, $thresholdsRef, $inputNamesRef, \@samples);
    close($out);
}


