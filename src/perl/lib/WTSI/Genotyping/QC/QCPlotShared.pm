# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# define shared constants and subroutines for QC plot scripts

package WTSI::Genotyping::QC::QCPlotShared;

use warnings;
use strict;
use Carp;
use FindBin qw($Bin);
use JSON;

# read default qc names and thresholds from .json files

# duplicate threshold is currently hard-coded in /software/varinf/bin/genotype_qc/pairwise_concordance_bed

sub meanSd {
    # find mean and standard deviation of input list
    # first pass -- mean
    my ($mean, $sd);
    unless (@_) {
	$mean = undef;
	$sd = 0;
    } else {
	my $total = 0;
	foreach my $x (@_) { $total+= $x; }
	$mean = $total / @_;
	# second pass -- sd
	$total = 0;
	foreach my $x (@_) { $total += abs($x - $mean); }
	$sd = $total / @_;
    }
    return ($mean, $sd);
}

sub readFileToString {
    # generic method to read a file (eg. json) into a single string variable
    my $inPath = shift();
    open IN, "< $inPath";
    my @lines = <IN>;
    close IN;
    return join('', @lines);
}

sub readQCFileNames {
    # read default qc file names
    my $inPath = shift();
    my %allNames = readQCNameConfig($inPath);
    my %fileNames = %{$allNames{'file_names'}};
    return %fileNames;
}

sub readQCNameConfig {
    # read qc metric names from JSON config
    my $inPath = shift();
    $inPath ||= $Bin."/../json/qc_name_config.json";
    my %names = %{decode_json(readFileToString($inPath))};
    return %names;
}

sub readQCMetricInputs {
    my $inPath = shift();
    my %names = readQCNameConfig($inPath);
    my %inputs = %{$names{'input_names'}};
    return %inputs;
}

sub readQCNameArray {
    my $inPath = shift();
    my %names = readQCNameConfig($inPath);
    my @nameArray = @{$names{'name_array'}};
    return @nameArray;
}

sub readQCNameHash {
    # convenience method, find hash for checking name legality
    my $inPath = shift();
    my @nameArray = readQCNameArray($inPath);
    my %nameHash;
    foreach my $name (@nameArray) { $nameHash{$name} = 1; }
    return %nameHash;
}

sub readQCShortNameHash {
    my $inPath = shift();
    my %names = readQCNameConfig($inPath);
    my %shortNames = %{$names{'short_names'}};
    return %shortNames;
}

sub readQCResultHash {
    # read QC results data structure from JSON file
    # assumes top-level structure is a hash
    my $inPath = shift;
    my %results = %{decode_json(readFileToString($inPath))};
    return %results;
}

sub readSampleData {
    # read data for given sample names from space-delimited file; return array of arrays of data read
    # optional start, stop points counting from zero
    my ($inPath, $startLine, $stopLine) = @_;
    unless (-e $inPath) { return (); } # silently return empty list if input does not exist
    $startLine ||= 0;
    $stopLine ||= 0;
    my @data;
    open IN, "< $inPath" || croak "Cannot open input path $inPath: $!";
    my $line = 0;
    while (<IN>) {
	$line++;
	if (/^#/ || $line <= $startLine) { next; } # comments start with a #
	elsif ($stopLine && $line+1 == $stopLine) { last; }
	my @fields = split;
	push(@data, \@fields);
    }
    close IN;
    return @data;    
}

sub readThresholds {
    # read QC metric thresholds from config path
    my $configPath = shift;
    my %config = %{decode_json(readFileToString($configPath))};
    my %thresholds = %{$config{"Metrics_thresholds"}};
    my %qcMetricNames = readQCNameHash();
    foreach my $name (keys(%thresholds)) { # validate metric names
	unless ($qcMetricNames{$name}) {
	    croak "Unknown QC metric name: $!";
	}
    }
    return %thresholds;
}

return 1;
