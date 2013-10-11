#! /software/bin/perl

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
use Carp;
use FindBin qw($Bin);
use Getopt::Long;
use IO::Uncompress::Gunzip; # for duplicate_full.txt.gz
use JSON;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultJsonConfig
                                               parseLabel
                                               getPlateLocationsFromPath
                                               readQCFileNames
                                               $UNKNOWN_PLATE
                                               $UNKNOWN_ADDRESS);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

my ($inputDir, $outputDir, $configPath, $dbPath, $iniPath, $help);

GetOptions("input-dir=s"   => \$inputDir,
	   "dbpath=s"      => \$dbPath,
	   "inipath=s"     => \$iniPath,
	   "output-dir=s"  => \$outputDir,
	   "config=s"      => \$configPath,
	   "help"          => \$help,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--input-dir         Directory containing input files (defaults to current working directory)
--output-dir        Output directory (defaults to input directory)
--config            Config path in JSON format (default taken from inipath)
--dbpath            Path to pipeline.db file, to obtain plate names for each sample
--inipath           Path to .ini file for pipeline database, and for JSON config default
--help              Print this help text and exit
";
    exit(0);
}

$inputDir ||= '.';
$outputDir ||= $inputDir;
$iniPath ||= $DEFAULT_INI;
$configPath ||= defaultJsonConfig($iniPath); 

if ((!$dbPath) && (!$iniPath)) { croak "Must supply at least one of pipeline database path and .ini path!"; }
if ($dbPath && !(-r $dbPath)) { croak "Cannot read pipeline database path $dbPath"; }
if ($iniPath && !(-r $iniPath)) { croak "Cannot read .ini path $iniPath"; }
if (not -r $configPath) { croak "Cannot read config path $configPath"; }

my %fileNames = readQCFileNames($configPath);
my $outPath = $outputDir.'/'.$fileNames{'qc_results'};

run($inputDir, $configPath, $dbPath, $iniPath, $outPath);

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

sub findMaxSimilarity {
    # find max pairwise similarity for each sample from duplicate_full.txt.gz
    my $inPath = shift;
    if (!(-e $inPath)) { croak "Input path $inPath does not exist!"; }
    my (%max, $GunzipError);
    my $z = new IO::Uncompress::Gunzip $inPath || 
        croak "gunzip failed: $GunzipError\n";
    while (<$z>) {
        chomp;
        my @words = split;
        my @samples = ($words[1], $words[2]);
        my $sim = $words[3];
        # redudant pairwise comparisons are omitted, so check both
        foreach my $sample (@samples) {
            my $lastMax = $max{$sample};
            if ((!$lastMax) || $sim > $lastMax) {
                $max{$sample} = $sim;
            }
        }
    }
    $z->close();
    return %max;
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
    } elsif ($metric eq 'magnitude') { 
        %results = resultsMagnitude($threshold, $inputDir.'/'.$input);
    } else { 
        carp "WARNING: Unknown QC metric $metric: $!"; 
    }
    return %results;
}

sub readSampleNames {
    # read sample names from sample_cr_het.txt (or any other tab-delimited file with sample name as first field)
    # Now obsolete for this script! But may be useful to preserve sample order in human-readable output.
    my $inPath = shift;
    open my $in, "<", $inPath || die "Cannot open input path $inPath: $!";
    my $i = 0;
    my @samples = ();
    while (<$in>) {
	if (/^#/) { next; } # comments start with a #
	chomp;
	my @words = split;
	push(@samples, shift(@words));
    }
    close $in;
    return @samples;
}


sub resultsCr {
    # find call rate (CR) and pass/fail status of each sample
    return resultsMinimum(@_);
}

sub resultsDuplicate {
    # find duplicate pairs from duplicate_summary.txt
    # exclude one member of each pair (the one with lower call rate; in a tie, the first "alphabetically")
    # read call rates from sample_cr_het.txt
    # $threshold is not used
    my ($threshold, $inPathsRef) = @_;
    my ($crHetPath, $duplicatePath, $duplicateFull) = @$inPathsRef;
    my (%duplicates, @pairs, %results);
    my %maxSimilarity = findMaxSimilarity($duplicateFull);
    # read duplicate sample names
    open my $in, "<", $duplicatePath || die "Cannot open input path $duplicatePath: $!";
    while (<$in>) {
        if (/^#/) { next; } # comments start with a #
        my @fields = split;
        my @pair = ($fields[1], $fields[2]);
        foreach my $sample (@pair) { $duplicates{$sample} = 1; }
        push(@pairs, \@pair);
    }
    close $in;
    # read call rates for duplicated samples
    my (%duplicateCR, @samples);
    my @data = WTSI::NPG::Genotyping::QC::QCPlotShared::readSampleData($crHetPath);
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
        if ($duplicateCR{$sam1} > $duplicateCR{$sam2}) { 
            $pass{$sam1} = 1; $pass{$sam2} = 0; 
        } elsif ($duplicateCR{$sam1} < $duplicateCR{$sam2}) { 
            $pass{$sam1} = 0; $pass{$sam2} = 1; 
        } 
        elsif ($sam1 lt $sam2) { 
            $pass{$sam1} = 1; $pass{$sam2} = 0; 
        } else { 
            $pass{$sam1} = 0; $pass{$sam2} = 1; 
        } 
    }
    # fill in results for all samples; metric = max similarity
    foreach my $sample (@samples) {
        my $metric = $maxSimilarity{$sample};
        if (!$duplicates{$sample}) { $results{$sample}=[1,$metric]; }
        elsif ($pass{$sample}) { $results{$sample} = [1,$metric]; }
        else { $results{$sample}=[0,$metric]; }
    }
    return %results;
}

sub resultsGender {
    # read gender results from sample_xhet_gender.txt
    # 'metric value' is concatenation of inferred, supplied gender codes
    # $threshold not used
    my ($threshold, $inPath) = @_;
    my @data = WTSI::NPG::Genotyping::QC::QCPlotShared::readSampleData($inPath, 1); # skip header on line 0
    my %results;
    foreach my $ref (@data) {
	my ($sample, $xhet, $inferred, $supplied) = @$ref;
	my $pass;
	if ($inferred==$supplied) { $pass = 1; }
	else { $pass = 0; }
	$results{$sample} = [$pass, $xhet, $inferred, $supplied];
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
    my @data = WTSI::NPG::Genotyping::QC::QCPlotShared::readSampleData($inPath);
    my %results;
    foreach my $ref (@data) {
	my @fields = @$ref;
	my ($sample, $concord, $status) = ($fields[0], $fields[3], $fields[4]);
	my $pass;
	if ($concord eq '.') {  # identity concordance not available -- assume pass
	    $pass = 1;
	    $concord = 'NA';
	} elsif ($status eq 'Skipped' || $concord >= $threshold) { 
	    $pass = 1; 
	} else { 
	    $pass = 0; 
	}
	if ($concord ne 'NA' && $concord == 1) { $concord = "1.0"; } # removes unwanted trailing zeroes
	$results{$sample} = [$pass, $concord];
    }
    return %results;
}

sub resultsMagnitude {
    return resultsMinimum(@_);
}
sub resultsMinimum {
    # read simple results file and apply minimum threshold
    # use for cr, normalised magnitude
    my ($threshold, $inPath) = @_;
    my @data = WTSI::NPG::Genotyping::QC::QCPlotShared::readSampleData($inPath);
    my %results;
    foreach my $ref (@data) {
        my @fields = @$ref;
        my ($sample, $metric) = ($fields[0], $fields[1]);
        my $pass;
        if ($metric >= $threshold) { $pass = 1; }
        else { $pass = 0; }
        $results{$sample} = [$pass, $metric];
    }
    return %results;
}


sub resultsMetricSd {
    # find results for given field, fail samples too far from the mean; use for het rate, xydiff
    # threshold expressed in standard deviations; first need to find absolute thresholds
    my ($threshold, $index, $inPath, $startLine) = @_;
    my (@samples, @values, $pass, %results);
    unless (-e $inPath) { return (); } # silently return empty hash if input does not exist
    my @data =  WTSI::NPG::Genotyping::QC::QCPlotShared::readSampleData($inPath, $startLine);
    foreach my $ref (@data) {
	my @fields = @$ref;
	push(@samples, $fields[0]);
	push(@values, $fields[$index]);
    }
    my ($mean, $sd) = WTSI::NPG::Genotyping::QC::QCPlotShared::meanSd(@values);
    my $min = $mean - $threshold*$sd;
    my $max = $mean + $threshold*$sd;
    for (my $i=0;$i<@samples;$i++) {
	if ($values[$i] >= $min && $values[$i] <= $max) { $pass = 1; }
	else { $pass = 0; }
	$results{$samples[$i]} = [$pass, $values[$i]];
    }
    return %results;
}

sub resultsXydiff {
    my ($threshold, $inPath) = @_;
    my $index = 1;
    my $startLine = 0;
    return resultsMetricSd($threshold, $index, $inPath, $startLine);
}

sub writeResults {
    # find QC results and write to given filehandle in JSON format
    # assumes all results will fit in memory!  If not, will need to rethink I/O and file format.
    my ($out, $inputDir, $dbPath, $iniPath, $metricsRef, $thresholdsRef, $inputNamesRef) = @_;
    my @metrics = @$metricsRef;
    my %thresholds = %$thresholdsRef;
    my %inputNames = %$inputNamesRef;
    my %metricResults;
    foreach my $metric (@metrics) {
        my %results = findMetricResults($inputDir, $metric, 
                                        $thresholds{$metric}, 
                                        $inputNames{$metric});
        $metricResults{$metric} = \%results;
    }
    # change from metric-major to sample-major ordering
    # if excluded samples are found, raise warning and remove from JSON output
    my %sampleResults = convertResults(\%metricResults);
    my %plateLocs = getPlateLocationsFromPath($dbPath, $iniPath);
    foreach my $sample (keys(%sampleResults)) {
        my %results = %{$sampleResults{$sample}};
        my $locsRef = $plateLocs{$sample};
	if (defined($locsRef)) { 
	    # samples with unknown location will have dummy values in hash
	    my ($plate, $addressLabel) = @$locsRef; 
	    $results{'plate'} = $plate;
	    $results{'address'} = $addressLabel;
	    $sampleResults{$sample} = \%results;
	} else { 
            # excluded sample has *no* location value
	    carp("Excluded sample $sample appears in QC metric data\n");
	    delete $sampleResults{$sample}; 
	}
    }
    my $resultString = encode_json(\%sampleResults);
    print $out $resultString;
    return 1;
}

sub run {
    # main method to run script
    my ($inputDir, $configPath, $dbPath, $iniPath, $outPath) = @_;
    my %thresholds = WTSI::NPG::Genotyping::QC::QCPlotShared::readThresholds($configPath);
    my @metricNames = WTSI::NPG::Genotyping::QC::QCPlotShared::readQCNameArray($configPath);
    my @metrics = ();
    foreach my $metric (@metricNames) { 
	# use metrics with defined thresholds
	if (defined($thresholds{$metric})) { push(@metrics, $metric); }
    } 
    my %inputNames = WTSI::NPG::Genotyping::QC::QCPlotShared::readQCMetricInputs($configPath);
    my $out;
    open($out, ">", $outPath) || die "Cannot open output path $outPath: $!"; 
    writeResults($out, $inputDir, $dbPath, $iniPath, \@metrics, \%thresholds, \%inputNames);
    close($out);
    return 1;
}


