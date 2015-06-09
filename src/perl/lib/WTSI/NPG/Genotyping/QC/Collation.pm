# Author:  Iain Bancarz, ib5@sanger.ac.uk
# February 2014

# Collate QC result outputs into a single JSON summary file
# Apply thresholds to find pass/fail status

package WTSI::NPG::Genotyping::QC::Collation;

use strict;
use warnings;
use Carp;
use IO::Uncompress::Gunzip qw($GunzipError); # for duplicate_full.txt.gz
use JSON;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(getDatabaseObject
                                               getPlateLocationsFromPath
                                               meanSd
                                               readQCMetricInputs
                                               readFileToString
                                               readSampleData);
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/collate readMetricThresholds/;

our $VERSION = '';

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

# metric names
our $CR_NAME = 'call_rate';
our $HET_NAME = 'heterozygosity';
our $DUP_NAME = 'duplicate';
our $ID_NAME = 'identity';
our $GENDER_NAME = 'gender';
our $XYD_NAME = 'xydiff';
our $MAG_NAME = 'magnitude';
our $LMH_NAME = 'low_maf_het';
our $HMH_NAME = 'high_maf_het';
# standard order for metric names
our @METRIC_NAMES = ($ID_NAME, $DUP_NAME, $GENDER_NAME, $CR_NAME, 
             $HET_NAME, $LMH_NAME, $HMH_NAME, $MAG_NAME, $XYD_NAME);
our @GENDERS = ('Unknown', 'Male', 'Female', 'Not_Available');
our $DUPLICATE_SUBSETS_KEY = 'SUBSETS';
our $DUPLICATE_RESULTS_KEY = 'RESULTS';
our %FILENAMES; # hash for names of input files

# Collate QC results from various output files into a single data structure,
# write JSON and CSV output files and update pipeline SQLite DB if required.

# Get additional information for .csv fields from pipeline DB:
# run,project,data_supplier,snpset,supplier_name,rowcol,beadchip_number,sample,include,plate,well,pass

sub addLocations {
    # add plate/well locations to a hash indexed by sample
    my %samples = %{ shift() };
    my ($dbPath, $iniPath) = @_;
    my %plateLocs = getPlateLocationsFromPath($dbPath, $iniPath);
    foreach my $uri (keys %samples) {
        my %results = %{$samples{$uri}};
        my $locsRef = $plateLocs{$uri};
        if (defined($locsRef)) { 
            # samples with unknown location will have dummy values in hash
            my ($plate, $addressLabel) = @$locsRef;
            $results{'plate'} = $plate;
            $results{'address'} = $addressLabel;
            $samples{$uri} = \%results;
        } else {
            # excluded sample has *no* location value
            print STDERR "Excluded sample URI $uri is in QC metric data\n";
        }
    }
    return \%samples;
}

sub appendNull {
    # append 'NA' values to given list
    my @array = @{ shift() };
    my $nulls = shift;
    for (my $i=0;$i<$nulls;$i++) {
        push(@array, 'NA');
    }
    return @array;
}

sub bySampleName {
    # comparison function for sorting samples in getSampleInfo
    # if in plate_well_id format, sort by id; otherwise use standard sort
    if ($a =~ /[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+/ &&
            $b =~ /[A-Za-z0-9]+_[A-Za-z0-9]+_[A-Za-z0-9]+/) {
        my @termsA = split(/_/, $a);
        my @termsB = split(/_/, $b);
        return $termsA[-1] cmp $termsB[-1];
    } else {
        return $a cmp $b;
    }
}

sub dbExcludedSamples {
    # find list of excluded sample URIs from database
    # use to fill in empty lines for CSV file
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my @excluded;
    my @samples = $db->sample->all;
    foreach my $sample (@samples) {
        if (!($sample->include)) {
            push @excluded, $sample->uri;
        }
    }
    $db->disconnect();
    return @excluded;
}

sub dbSampleInfo {
    # get general information on analysis run from pipeline database
    # return a hash indexed by sample
    my $dbfile = shift;
    my $db = getDatabaseObject($dbfile);
    my %sampleInfo;
    my @runs = $db->piperun->all;
    foreach my $run (@runs) {
        my @root;
        my @datasets = $run->datasets->all;
        foreach my $dataset (@datasets) {
            my @samples = $dataset->samples->all;
            @root = ($run->name, $dataset->if_project,
                     $dataset->datasupplier->name,
                     $dataset->snpset->name);
            # query for rowcol, supplier name, chip no.
            foreach my $sample (@samples) {
                my @info = (
                    $sample->rowcol,
                    $sample->beadchip,
                    $sample->supplier_name,
                    $sample->cohort);
                foreach (my $i=0;$i<@info;$i++) { # set null values to "NA"
                    if ($info[$i] eq "") { $info[$i] = "NA"; }
                }
                unshift(@info, @root);
                $sampleInfo{$sample->uri} = \@info;
            }
        }
    }
    $db->disconnect();
    return %sampleInfo;
}

sub duplicateSubsets {
    # Find *connected subsets* of the duplicate pairs:
    # if A<->B and B<->C then A~B, B~C and A~C
    # where <-> denotes similarity on snp panel greater than some threshold,
    # and ~ denotes membership of a connected subset (equivalence class).
    #
    # The member of a connected subset with the highest call rate is kept;
    # others are flagged as QC failures. This is a "quick and dirty"
    # substitute for applying a clustering algorithm to find subsets with
    # high mutual similarity. It should give acceptable results, but for
    # very high duplicate rates, it will fail *more* samples than
    # a clustering algorithm would.
    #
    # Arguments: - Hash of hashes of pairwise similarities
    #            - Similarity threshold for duplicates
    # Return value: list of lists of samples in each subset
    my %similarity = %{ shift() };
    my $threshold = shift;
    my @samples = keys(%similarity);
    # if sample has no neighbours: simple, it is in a subset by itself
    # if sample does have neighbours: add to appropriate subset
    my @subsets;
    foreach my $sample_i (@samples) {
        my $added = 0;
        SUBSET: for (my $i=0;$i<@subsets;$i++) {
            my @subset = @{$subsets[$i]};
            foreach my $sample_j (@subset) {
                if ($similarity{$sample_i}{$sample_j} >= $threshold) {
                    push(@subset, $sample_i);
                    $subsets[$i] = [ @subset ];
                    $added = 1;
                    last SUBSET;
                }
            }
        }
        unless ($added) { push(@subsets, [$sample_i]); }
    }
    return @subsets;
}

sub evaluateThresholds {
    # apply thresholds, evaluate pass/fail status of each sample/metric pair
    # input a sample-major hash of metric values
    # return reference to a hash in 'qc_results.json' format (minus plate name and address for each sample)
    my ($sampleResultsRef, $thresholdsRef) = @_;
    my %results = %{$sampleResultsRef};
    my %thresholds = %{$thresholdsRef};
    my %evaluated = ();
    foreach my $sample (keys(%results)) {
        my %result = %{$results{$sample}};
        foreach my $metric (keys(%result)) {
            if (!defined($thresholds{$metric})) {
                croak "No thresholds defined for metric $metric: $!";
            }
            my $value = $result{$metric};
            my $pass = metricPass($metric, $value, $thresholds{$metric});
            my @terms = ($pass, );
            if ($metric eq $GENDER_NAME) { push (@terms, @{$value}); } 
            elsif ($metric eq $DUP_NAME) { push (@terms, @{$value}[0]); }
            else { push(@terms, $value); }
            $evaluated{$sample}{$metric} = \@terms;
        }
    }
    return \%evaluated;
}

sub excludedSampleCsv {
    # generate CSV lines for samples excluded from pipeline DB
    my @sampleNames = @{ shift() };
    my %sampleInfo = %{ shift() };   # generic sample/dataset info
    my %metrics = %{ shift() };
    my %excluded = %{ shift() };
    my @lines = ();
    foreach my $sample (@sampleNames) {
        if (!$excluded{$sample}) { next; }
        my @fields = @{$sampleInfo{$sample}};
        push(@fields, $sample);
        push(@fields, 'Excluded'); 
        @fields = appendNull(\@fields, 3); # null plate, well, sample pass
        foreach my $name (@METRIC_NAMES) {
            if (!$metrics{$name}) { 
                next;
            } elsif ($name eq $GENDER_NAME) {
                @fields = appendNull(\@fields, 4); # pass/fail, metric triple
            } else {
                @fields = appendNull(\@fields, 2); # pass/fail, metric
            }
        }
        push(@lines, join(',', @fields));
    }
    return \@lines;
}

sub findMetricResults {
    # find QC results in metric-major order, return a hash reference
    # "results" for gender, duplicate, and identity are complex! represent as lists. For other metrics, result is a single float. See methods in write_qc_status.pl
    my $inputDir = shift;
    my @metricNames = @{ shift() };
    my %allResults;
    foreach my $name (@metricNames) {
    my $resultsRef;
    if ($name eq $CR_NAME) {
        $resultsRef = resultsCallRate($inputDir);
    } elsif ($name eq $DUP_NAME) {
        $resultsRef = resultsDuplicate($inputDir);
    } elsif ($name eq $GENDER_NAME) {
        $resultsRef = resultsGender($inputDir);
    } elsif ($name eq $HET_NAME) {
        $resultsRef = resultsHet($inputDir);
    } elsif ($name eq $HMH_NAME) {
        $resultsRef = resultsHighMafHet($inputDir);
    } elsif ($name eq $ID_NAME) {
        $resultsRef = resultsIdentity($inputDir);
    } elsif ($name eq $LMH_NAME) {
        $resultsRef = resultsLowMafHet($inputDir);
    } elsif ($name eq $MAG_NAME) {
        $resultsRef = resultsMagnitude($inputDir);
    } elsif ($name eq $XYD_NAME) {
        $resultsRef = resultsXydiff($inputDir);
    } else {
        croak "Unknown metric name $name for results: $!";
    }
    if ($resultsRef) { $allResults{$name} = $resultsRef; }
    }
    return \%allResults;
}

sub findThresholds {
    # find threshold values, which may depend on mean/sd of metric values
    my %metricResults = %{ shift() };
    my %thresholdsConfig = %{ shift() };
    my %thresholds;
    my @names = keys(%metricResults);
    foreach my $metric (keys(%metricResults)) {
        if ($metric eq $HET_NAME || $metric eq $LMH_NAME || $metric eq $HMH_NAME || $metric eq $XYD_NAME) {
            # find mean/sd for thresholds
            my %resultsBySample = %{$metricResults{$metric}};
            my ($mean, $sd) = meanSd(values(%resultsBySample));
            my $min = $mean - ($thresholdsConfig{$metric}*$sd);
            my $max = $mean + ($thresholdsConfig{$metric}*$sd);
            $thresholds{$metric} = [$min, $max];
        } elsif ($metric eq $CR_NAME || $metric eq $DUP_NAME || $metric eq $ID_NAME || $metric eq $GENDER_NAME || $metric eq $MAG_NAME ) {
            $thresholds{$metric} = $thresholdsConfig{$metric};
        } else {
            croak "Unknown metric name $metric for thresholds: $!";
        }
    }
    return %thresholds;
}

sub includedSampleCsv {
    # generate CSV lines for samples included in pipeline DB
    my @sampleNames = @{ shift() }; 
    my %sampleInfo = %{ shift() };   # generic sample/dataset info
    my %passResult = %{ shift() }; # metric pass/fail status and values
    my %samplePass = %{ shift() }; # overall pass/fail by sample
    my %excluded = %{ shift() };
    my %metrics;
    my @lines = ();
    foreach my $sample (@sampleNames) {
        if ($excluded{$sample}) { next; }
        my @fields = @{$sampleInfo{$sample}}; # start with general info
        my %result = %{$passResult{$sample}};
        # first obtain: sample include plate well pass
        push(@fields, $sample);
        push(@fields, 'Included');
        push(@fields, $result{'plate'});
        push(@fields, $result{'address'}); # aka well
        if ($samplePass{$sample}) { push(@fields, 'Pass'); }
        else { push(@fields, 'Fail'); }
        # now add relevant metric values
        foreach my $metric (@METRIC_NAMES) {
            if (!defined($result{$metric})) { next; }
            $metrics{$metric} = 1;
            my @metricResult = @{$result{$metric}}; # pass/fail, value(s)
            if ($metricResult[0]) { $metricResult[0] = 'Pass'; }
            else { $metricResult[0] = 'Fail'; }
            if ($metric eq $GENDER_NAME) { # use human-readable gender names
                $metricResult[2] = $GENDERS[$metricResult[2]];
                $metricResult[3] = $GENDERS[$metricResult[3]];
            }
            push (@fields, @metricResult);
        }
        push(@lines, join(',', @fields));
    }
    return (\@lines, \%metrics);
}

sub metricPass {
    # find pass/fail status for given metric, value, and threshold
    my ($metric, $value, $threshold) = @_;
    my $pass = 0;
    if ($metric eq $CR_NAME || $metric eq $MAG_NAME) {
        if ($value >= $threshold) { $pass = 1; }
    } elsif ($metric eq $DUP_NAME) {
        my ($similarity, $keep) = @{$value};
        if ($similarity < $threshold || $keep) { $pass = 1; }
    } elsif ($metric eq $GENDER_NAME) {
        my ($xhet, $inferred, $supplied) = @{$value};
        if ($inferred==$supplied) { $pass = 1; }
    } elsif ($metric eq $HET_NAME || $metric eq $LMH_NAME || 
                 $metric eq $HMH_NAME || $metric eq $XYD_NAME) {
        my ($min, $max) = @{$threshold};
        if ($value >= $min && $value <= $max) { $pass = 1; }
    } elsif ($metric eq $ID_NAME) {
        if ($value eq 'NA' || $value > $threshold) { $pass = 1; }
    } else {
        croak "Unknown metric name: $!";
    }
    return $pass;
}

sub passFailBySample {
    # evaluate results by metric and find overall pass/fail for each sample
    my %results = %{ shift() };
    my $crHetPath = shift(); # required to find duplicates pass/fail
    my %passFail = ();
    foreach my $sample (keys(%results)) {
        my %result = %{$results{$sample}};
        my $samplePass = 1;
        foreach my $metric (@METRIC_NAMES) {
            if (!defined($result{$metric})) { next; }
            my @values = @{$result{$metric}};
            my $pass = shift @values;
            if (!$pass) { $samplePass = 0; last; }
        }
        $passFail{$sample} = $samplePass;
    }
    return %passFail;
}

sub processDuplicates {
    # pre-processing of duplicate metric with given threshold
    # partition samples into subsets; sample in subset with highest CR passes
    # want to write partitioning and pass/fail status to file
    # then read file for final metric/threshold collation
    my $inputDir = shift;
    my $threshold = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'duplicate'};
    if (!(-e $inPath)) { 
        croak "Input path for duplicates \"$inPath\" does not exist: $!";
    }
    my ($simRef, $maxRef) = readDuplicates($inPath);
    my @subsets = duplicateSubsets($simRef, $threshold);
    my %max = %{$maxRef};
    # read call rates and find keep/discard status
    my %cr = %{resultsCallRate($inputDir)};
    my %results;
    foreach my $subsetRef (@subsets) {
        my $maxCR = 0;
        my @subset = @{$subsetRef};
        # first pass -- find highest CR
        foreach my $sample (@subset) {
            if ($cr{$sample} > $maxCR) { $maxCR = $cr{$sample}; }
        }
        # second pass -- record sample status
        # may keep more than one sample if there is a tie for greatest CR
        foreach my $sample (@subset) {
            my $keep = 0;
            if ($cr{$sample} eq $maxCR) { $keep = 1; }
            $results{$sample} = [$max{$sample}, $keep];
        }
    }
    my %output;
    $output{$DUPLICATE_SUBSETS_KEY} = \@subsets;
    $output{$DUPLICATE_RESULTS_KEY} = \%results;
    my $outPath = $inputDir.'/'.$FILENAMES{'duplicate_subsets'};
    open my $out, ">", $outPath || croak "Cannot open output '$outPath'";
    print $out to_json(\%output);
    close $out || croak "Cannot close output '$outPath'";
}

sub readDuplicates {
    # read pairwise similarities for duplicate check from gzipped file
    # also find maximum pairwise similarity for each sample
    my $inPath = shift;
    my (%similarity, %max);
    my $z = new IO::Uncompress::Gunzip $inPath ||
    croak "gunzip failed: $GunzipError\n";
    my $firstLine = 1;
    while (<$z>) {
        if ($firstLine) { $firstLine = 0; next; } # skip headers
        chomp;
        my @words = split;
        my @samples = ($words[1], $words[2]);
        my $sim = $words[3]; # similarity on SNP panel
        $similarity{$samples[0]}{$samples[1]} = $sim;
        $similarity{$samples[1]}{$samples[0]} = $sim;
    }
    $z->close();
    # find max pairwise similarity for each sample
    foreach my $sample_i (keys(%similarity)) {
        my $maxSim = 0;
        foreach my $sample_j (keys(%similarity)) {
            if ($sample_i eq $sample_j) { next; }
            my $sim = $similarity{$sample_i}{$sample_j};
            if ($sim > $maxSim) { $maxSim = $sim; }
        }
        $max{$sample_i} = $maxSim;
    }
    return (\%similarity, \%max);
}

sub readMetricThresholds {
    # exportable convenience method to read metric thresholds from JSON config
    my $configPath = shift;
    my %config = %{decode_json(readFileToString($configPath))};
    my %thresholds = %{$config{'Metrics_thresholds'}};
    return \%thresholds;
}

sub resultsCallRate {
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'call_rate'};
    if (!(-e $inPath)) { 
        croak "Input path for call rate \"$inPath\" does not exist: $!";
    }
    my $index = 1;
    return resultsSpaceDelimited($inPath, $index);
}

sub resultsDuplicate {
    # read pre-processed values for duplicate metric
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'duplicate_subsets'};
    if (!(-e $inPath)) { 
        croak "Input path for duplicates \"$inPath\" does not exist: $!";
    }
    open my $in, "<", $inPath || croak "Cannot open input '$inPath'";
    my $input = <$in>;
    close $in || croak "Cannot close input '$inPath'";
    my %duplicateData = %{ from_json($input) };
    my $resultsRef = $duplicateData{$DUPLICATE_RESULTS_KEY};
    return $resultsRef;
}

sub resultsGender {
    # read gender results from sample_xhet_gender.txt
    # 'metric value' is concatenation of inferred, supplied gender codes
    # $threshold not used
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'gender'};
    if (!(-e $inPath)) { 
        croak "Input path for gender \"$inPath\" does not exist: $!";
    }
    my @data = WTSI::NPG::Genotyping::QC::QCPlotShared::readSampleData($inPath, 1); # skip header on line 0
    my %results;
    foreach my $ref (@data) {
        my ($sample, $xhet, $inferred, $supplied) = @$ref;
        $results{$sample} = [$xhet, $inferred, $supplied];
    }
    return \%results;
}

sub resultsHet {
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'heterozygosity'};
    if (!(-e $inPath)) { 
        croak "Input path for heterozygosity \"$inPath\" does not exist: $!";
    }
    my $index = 2;
    return resultsSpaceDelimited($inPath, $index);
}

sub resultsHighMafHet {
    my $inputDir = shift;
    return resultsMafHet($inputDir, 1);
}

sub resultsIdentity {
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'identity'};
    my %data = %{decode_json(readFileToString($inPath))};
    return $data{'results'};
}


sub resultsLowMafHet {
    my $inputDir = shift;
    return resultsMafHet($inputDir, 0);
}

sub resultsMafHet {
    # read JSON file output by Plinktools het_by_maf.py
    my $inputDir = shift;
    my $high = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'het_by_maf'};
    if (!(-r $inPath)) {
        carp "Omitting MAF heterozygosity; cannot read input \"$inPath\": $!";
        return 0;
    }
    my %data = %{decode_json(readFileToString($inPath))};
    my %results;
    foreach my $sample (keys(%data)) {
        # TODO modify output format of het_by_maf.py
        if ($high) { $results{$sample} = $data{$sample}{'high_maf_het'}[1]; }
        else { $results{$sample} = $data{$sample}{'low_maf_het'}[1]; }
    }
    return \%results;
}

sub resultsMagnitude {
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'magnitude'};
    if (!(-e $inPath)) {
        carp "Omitting magnitude; input \"$inPath\" does not exist: $!";
        return 0; # magnitude of intensity is optional
    }
    my $index = 1;
    return resultsSpaceDelimited($inPath, $index);
}

sub resultsSpaceDelimited {
    # read metric results from a space-delimited file ()
    my $inPath = shift;
    my $index = shift;
    my @data = readSampleData($inPath);
    my %results;
    foreach my $ref (@data) {
        my @fields = @{$ref};
        my $uri = $fields[0];
        my $metric = $fields[$index];
        $results{$uri} = $metric;
    }
    return \%results;
}

sub resultsXydiff {
    my $inputDir = shift;
    my $inPath = $inputDir.'/'.$FILENAMES{'xydiff'};
    if (!(-e $inPath)) { 
        carp "Omitting xydiff; input \"$inPath\" does not exist: $!";
        return 0;
    }
    my $index = 1;
    return resultsSpaceDelimited($inPath, $index);
}

sub transposeResults {
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
    return \%sampleResults;
}

sub updateDatabase {
    # update pipeline db with pass/fail of each sample
    my $dbPath = shift;
    my %samplePass = %{ shift() };
    # samples which were previously excluded should *remain* excluded
    my $db = getDatabaseObject($dbPath); # uses default .ini path
    my @samples = $db->sample->all;
    $db->in_transaction(sub {
                            foreach my $sample (@samples) {
                                my $uri = $sample->uri;
                                if (!($samplePass{$uri})) {
                                    $sample->update({'include' => 0});
                                }
                            }
                        });
    $db->disconnect();
}

sub writeCsv {
    # write a .csv file summarizing metrics and pass/fail status
    my $csvPath = shift;
    my %sampleInfo = %{ shift() }; # generic sample/dataset info
    my @excluded = @{ shift() };   # samples excluded in DB
    my %passResult = %{ shift() }; # metric pass/fail status and values
    my %samplePass = %{ shift() }; # overall pass/fail by sample
    my %excluded;
    foreach my $sample (@excluded) { $excluded{$sample} = 1; }
    my @lines = ();
    my @sampleNames = keys(%sampleInfo);
    @sampleNames = sort bySampleName @sampleNames;
    my ($linesRef, $metricsRef);
    # first pass; append lines for samples included in pipeline DB
    ($linesRef, $metricsRef) = includedSampleCsv(\@sampleNames, \%sampleInfo,
                                                 \%passResult, \%samplePass,
                                                 \%excluded);
    push(@lines, @{$linesRef});
    # second pass; append dummy lines for excluded samples
    $linesRef = excludedSampleCsv(\@sampleNames, \%sampleInfo,
                                  $metricsRef, \%excluded);
    push(@lines, @{$linesRef});
    my %metrics = %{$metricsRef};
    # use %metrics to construct appropriate CSV header
    my @headers = qw/run project data_supplier snpset rowcol beadchip_number 
                     supplier_name cohort sample include plate well pass/;
    foreach my $name (@METRIC_NAMES) {
        my @suffixes;
        if (!$metrics{$name}) { 
            next; 
        } elsif ($name eq $GENDER_NAME) {
            @suffixes = qw/pass xhet inferred supplied/;
        } else {
            @suffixes = qw/pass value/;
        }
        foreach my $suffix (@suffixes) { push(@headers, $name.'_'.$suffix); }
    }
    unshift(@lines, join(',', @headers));
    # write results to file
    open my $out, ">", $csvPath || croak "Cannot open output $csvPath: $!";
    foreach my $line (@lines) { print $out $line."\n"; }
    close $out || croak "Cannot close output $csvPath: $!";
}

sub writeJson {
    # open a file, and write the given reference in JSON format
    my ($outPath, $dataRef) = @_;
    my $resultString = encode_json($dataRef);
    open my $out, ">", $outPath ||
        croak "Cannot open output path $outPath: $!";
    print $out $resultString;
    close($out) || croak "Cannot close output path $outPath: $!";;
    return 1;
}

sub collate {
    # main method to collate results and write outputs
    # $metricsRef is an optional reference to an array of metric names; use to specify a subset of metrics for evaluation
    my ($inputDir, $configPath, $thresholdPath, $dbPath, $iniPath,
        $statusJson, $metricsJson, $csvPath, $exclude, $metricsRef,
        $verbose) = @_;
    my (%config, %thresholdConfig, @metricNames);
    if ($verbose) { print STDERR "Started collating QC results.\n";    }
    %thresholdConfig = %{readMetricThresholds($thresholdPath)};
    if ($metricsRef) { 
        @metricNames = @{$metricsRef}; 
        foreach my $name (@metricNames) {
            if (!defined($thresholdConfig{$name})) {
                croak "No threshold defined for metric $name: $!";
            }
        }
    }
    else {
        @metricNames = keys(%thresholdConfig);
    }
    %config = %{decode_json(readFileToString($configPath))};
    %FILENAMES = %{$config{'collation_names'}};

    # 0) reprocess duplicate results for given threshold
    processDuplicates($inputDir, $thresholdConfig{$DUP_NAME});

    # 1) find metric values (and write to file if required)
    my $metricResultsRef = findMetricResults($inputDir, \@metricNames);
    my $sampleResultsRef = transposeResults($metricResultsRef);
    if ($verbose) { print "Found metric values.\n"; }
    if ($metricsJson) { writeJson($metricsJson, $sampleResultsRef);  }
    if ($statusJson || $csvPath || $exclude) {
        # if output options require evaluation of thresholds
        # 2) apply filters to find pass/fail status
        my %thresholds = findThresholds($metricResultsRef, \%thresholdConfig);
        my $passResultRef = evaluateThresholds($sampleResultsRef,
                                               \%thresholds);
        if ($verbose) { print "Evaluated pass/fail status.\n"; }

        # 3) add location info and write JSON status file
        $passResultRef = addLocations($passResultRef, $dbPath, $iniPath);
        writeJson($statusJson, $passResultRef);
        if ($verbose) { print "Wrote status JSON file $statusJson.\n"; }

        # 4) write CSV (if required)
        my %samplePass = passFailBySample($passResultRef);
        if ($csvPath) { 
            my %sampleInfo = dbSampleInfo($dbPath);
            my @excluded = dbExcludedSamples($dbPath);
            writeCsv($csvPath, \%sampleInfo, \@excluded, $passResultRef,
                     \%samplePass);
            if ($verbose) { print "Wrote CSV $csvPath.\n"; }
        }

        # 5) exclude failing samples in pipeline DB (if required)
        if ($exclude) { updateDatabase($dbPath, \%samplePass); }
        if ($verbose) { print "Updated pipeline DB $dbPath.\n"; }
    }
}

1;
