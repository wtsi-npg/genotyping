
package WTSI::NPG::Genotyping::QC::Collator;

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# February 2014

# Collate QC result outputs into a single JSON summary file
# Apply thresholds to find pass/fail status

use strict;
use warnings;
use File::Slurp qw(read_file);
use IO::Uncompress::Gunzip qw($GunzipError); # for duplicate_full.txt.gz
use JSON;
use Moose;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(getDatabaseObject
                                               getPlateLocationsFromPath
                                               meanSd
                                               readQCMetricInputs
                                               readSampleData);

use Data::Dumper; # FIXME

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

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
our $UNKNOWN_PLATE = "Unknown_plate";
our $UNKNOWN_ADDRESS = "Unknown_address";

# Collate QC results from various output files into a single data structure,
# write JSON and CSV output files and update pipeline SQLite DB if required.

# Get additional information for .csv fields from pipeline DB:
# run,project,data_supplier,snpset,supplier_name,rowcol,beadchip_number,sample,include,plate,well,pass

has 'db_path' =>
  (is         => 'ro',
   isa        => 'Str',
   required   => 1);

has 'ini_path' =>
  (is         => 'ro',
   isa        => 'Str',
   required   => 1,
   default    => $ENV{HOME} . "/.npg/genotyping.ini" );

has 'input_dir' =>
  (is         => 'ro',
   isa        => 'Str',
   required   => 1, );

has 'config_path' =>
  (is         => 'ro',
   isa        => 'Str',
   required   => 1,
   documentation => 'Path to a JSON file with required parameters.'
);

has 'filter_path' =>
  (is         => 'ro',
   isa        => 'Str',
   documentation => 'Path to a JSON file with parameters to determine '.
       'sample exclusion. Optional, overrides values in config_path.'
);

has 'duplicate_subsets_path' =>
  (is         => 'ro',
   isa        => 'Str',
   lazy       => 1,
   default    => sub {
       my ($self,) = @_; 
       return $self->input_dir.'/'.$self->filenames->{'duplicate_subsets'};
   },
   documentation => 'Path for output of sample subsets from the '.
       'duplicate check.'
);

has 'db'  =>
  (is         => 'ro',
   isa        => 'WTSI::NPG::Genotyping::Database::Pipeline',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_db');

has 'threshold_parameters' =>
  (is         => 'ro',
   isa        => 'HashRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_threshold_parameters',
   documentation => 'Parameters to determine pass/fail thresholds for '.
       'each metric. The actual threshold values may vary; for example, '.
       'some thresholds are defined in terms of standard deviations '.
       'from the mean.'
);

has 'metrics' =>
  (is         => 'ro',
   isa        => 'ArrayRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_metrics');

has 'filenames' =>
  (is         => 'ro',
   isa        => 'HashRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_filenames');

sub addLocations {
    # add plate/well locations to a hash indexed by sample
    my ($self, $samplesRef) = @_;
    my %samples = %{$samplesRef};
    my ($dbPath, $iniPath) = @_;
    my %plateLocs = $self->plateLocations();
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
            $self->logwarn('Excluded sample URI ', $uri,
                           'is in QC metric data');
        }
    }
    return \%samples;
}

sub appendNull {
    # append 'NA' values to given list
    my ($self, $arrayRef, $nullTotal) = @_;
    my @array = @{$arrayRef};
    for (my $i=0;$i<$nullTotal;$i++) {
        push(@array, 'NA');
    }
    return @array;
}

sub dbExcludedSamples {
    # find list of excluded sample URIs from database
    # use to fill in empty lines for CSV file
    my ($self, ) = @_;
    my @excluded;
    $self->db->connect(RaiseError => 1,
                       on_connect_do => 'PRAGMA foreign_keys = ON');
    my @samples = $self->db->sample->all;
    foreach my $sample (@samples) {
        if (!($sample->include)) {
            push @excluded, $sample->uri;
        }
    }
    $self->db->disconnect();
    return @excluded;
}

sub dbSampleInfo {
    # get general information on analysis run from pipeline database
    # return a hash indexed by sample
    my ($self, ) = @_;
    my %sampleInfo;
    $self->db->connect(RaiseError => 1,
                       on_connect_do => 'PRAGMA foreign_keys = ON');
    my @runs = $self->db->piperun->all;
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
    $self->db->disconnect();
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
    my ($self, $similarityRef) = @_;
    my %similarity = %{$similarityRef};
    my @samples = keys(%similarity);
    my $threshold = $self->threshold_parameters->{$DUP_NAME};
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
    my ($self, $sampleResultsRef, $thresholdsRef) = @_;
    my %results = %{$sampleResultsRef};
    my %thresholds = %{$thresholdsRef};
    my %evaluated = ();
    foreach my $sample (keys(%results)) {
        my %result = %{$results{$sample}};
        foreach my $metric (keys(%result)) {
            if (!defined($thresholds{$metric})) {
                $self->logcroak("No thresholds defined for metric '",
                                $metric, "'");
            }
            my $value = $result{$metric};
            my $pass = $self->metricPass($metric, $value,
                                         $thresholds{$metric});
            my @terms = ($pass, );
            if ($metric eq $GENDER_NAME || $metric eq $ID_NAME) {
                push (@terms, @{$value});
            } elsif ($metric eq $DUP_NAME) {
                push (@terms, @{$value}[0]);
            } else {
                push(@terms, $value);
            }
            $evaluated{$sample}{$metric} = \@terms;
        }
    }
    return \%evaluated;
}

sub excludedSampleCsv {
    # generate CSV lines for samples excluded from pipeline DB
    my ($self, $sampleNamesRef, $sampleInfoRef,
        $metricsRef, $excludedRef) = @_;
    my @sampleNames = @{$sampleNamesRef};
    my %sampleInfo = %{$sampleInfoRef};   # generic sample/dataset info
    my %metrics = %{$metricsRef};
    my %excluded = %{$excludedRef};
    my @lines = ();
    foreach my $sample (@sampleNames) {
        if (!$excluded{$sample}) { next; }
        my @fields = @{$sampleInfo{$sample}};
        push(@fields, $sample);
        push(@fields, 'Excluded'); 
        @fields = $self->appendNull(\@fields, 3); # null plate, well, pass
        foreach my $name (@METRIC_NAMES) {
            if (!$metrics{$name}) {
                next;
            } elsif ($name eq $GENDER_NAME) {
                # pass/fail, metric triple
                @fields = $self->appendNull(\@fields, 4);
            } elsif ($name eq $ID_NAME) {
                # pass/fail, metric double
                @fields = $self->appendNull(\@fields, 3);
            } else {
                # pass/fail, metric
                @fields = $self->appendNull(\@fields, 2);
            }
        }
        push(@lines, join(',', @fields));
    }
    return \@lines;
}

sub findMetricResults {
    # find QC results in metric-major order, return a hash reference
    # "results" for gender, duplicate, and identity are complex! represent as lists. For other metrics, result is a single float. See methods in write_qc_status.pl
    my ($self,) = @_;
    my %allResults;
    foreach my $name (@{$self->metrics}) {
        my $resultsRef;
        if ($name eq $CR_NAME) {
            $resultsRef = $self->resultsCallRate();
        } elsif ($name eq $DUP_NAME) {
            $resultsRef = $self->resultsDuplicate();
        } elsif ($name eq $GENDER_NAME) {
            $resultsRef = $self->resultsGender();
        } elsif ($name eq $HET_NAME) {
            $resultsRef = $self->resultsHet();
        } elsif ($name eq $HMH_NAME) {
            $resultsRef = $self->resultsHighMafHet();
        } elsif ($name eq $ID_NAME) {
            $resultsRef = $self->resultsIdentity();
        } elsif ($name eq $LMH_NAME) {
            $resultsRef = $self->resultsLowMafHet();
        } elsif ($name eq $MAG_NAME) {
            $resultsRef = $self->resultsMagnitude();
        } elsif ($name eq $XYD_NAME) {
            $resultsRef = $self->resultsXydiff();
        } else {
            $self->logcroak("Unknown metric name $name for results: $!");
        }
        if ($resultsRef) { $allResults{$name} = $resultsRef; }
    }
    return \%allResults;
}

sub findThresholds {
    # find threshold values, which may depend on mean/sd of metric values
    my ($self, $metricResultsRef) = @_;
    my %metricResults = %{$metricResultsRef};
    my %thresholds;
    my @names = keys(%metricResults);
    foreach my $metric (keys(%metricResults)) {
        if ($metric eq $HET_NAME || $metric eq $LMH_NAME || $metric eq $HMH_NAME || $metric eq $XYD_NAME) {
            # find mean/sd for thresholds
            my %resultsBySample = %{$metricResults{$metric}};
            my ($mean, $sd) = meanSd(values(%resultsBySample));
            my $min = $mean - ($self->threshold_parameters->{$metric}*$sd);
            my $max = $mean + ($self->threshold_parameters->{$metric}*$sd);
            $thresholds{$metric} = [$min, $max];
        } elsif ($metric eq $CR_NAME || $metric eq $DUP_NAME || $metric eq $ID_NAME || $metric eq $GENDER_NAME || $metric eq $MAG_NAME ) {
            $thresholds{$metric} = $self->threshold_parameters->{$metric};
        } else {
            $self->logcroak("Unknown metric name '", $metric,
                            "' for thresholds");
        }
    }
    return \%thresholds;
}

sub includedSampleCsv {
    # generate CSV lines for samples included in pipeline DB
    my $self = shift; # TODO fix argument parsing
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
                # 'supplied' Plink gender may be -9 or other arbitrary number
                my $totalCodes = scalar @GENDERS;
                if ($metricResult[3] < 0 || $metricResult[3] >= $totalCodes){
                    $metricResult[3] = $totalCodes - 1; # 'not available'
                }
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
    my ($self, $metric, $value, $threshold) = @_;
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
        my ($probability, $concordance) = @{$value};
        if ($value eq 'NA' || $probability > $threshold) { $pass = 1; }
    } else {
        $self->logcroak("Unknown metric name '", $metric,
                        "' for pass/fail evaluation");
    }
    return $pass;
}

sub passFailBySample {
    # evaluate results by metric and find overall pass/fail for each sample
    my ($self, $resultsRef) = @_;
    my %results = %{$resultsRef};
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

sub plateLocations {
    my ($self,) = @_;
    $self->db->connect(RaiseError => 1,
                       on_connect_do => 'PRAGMA foreign_keys = ON');
    my @samples = $self->db->sample->all;
    my %plateLocs;
    $self->db->in_transaction(
        sub {
            foreach my $sample (@samples) {
                my ($plate, $x, $y) = (0,0,0);
                my $uri = $sample->uri;
                if (!defined($uri)) {
                    $self->logwarn("Sample '$sample' has no uri!");
                next;
                } elsif ($sample->include == 0) {
                    next; # excluded sample
                }
                # assume one well per sample
                my $well = ($sample->wells->all)[0];
                if (defined($well)) { 
                    my $address = $well->address;
                    my $label = $address->label1;
                    $plate = $well->plate;
                    my $plateName = $plate->ss_barcode;
                    $plateLocs{$uri} = [$plateName, $label];
                } else {
                    $plateLocs{$uri} = [$UNKNOWN_PLATE, $UNKNOWN_ADDRESS];
                }
            }
        });
    $self->db->disconnect();
    return %plateLocs;
}

sub processDuplicates {
    # pre-processing of duplicate metric with given threshold
    # partition samples into subsets; sample in subset with highest CR passes
    # want to write partitioning and pass/fail status to file
    # then read file for final metric/threshold collation
    my ($self, $threshold) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'duplicate'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for duplicates '",
                        $inPath, "' does not exist");
    }
    my ($simRef, $maxRef) = $self->readDuplicates($inPath);
    my @subsets = $self->duplicateSubsets($simRef, $threshold);
    my %max = %{$maxRef};
    # read call rates and find keep/discard status
    my %cr = %{$self->resultsCallRate()};
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
    my $outPath = $self->duplicate_subsets_path;
    open my $out, ">", $outPath ||
        $self->logcroak("Cannot open output '$outPath'");
    print $out to_json(\%output);
    close $out ||
        $self->logcroak("Cannot close output '$outPath'");
}

sub readDuplicates {
    # read pairwise similarities for duplicate check from gzipped file
    # also find maximum pairwise similarity for each sample
    my ($self, $inPath) = @_;
    my (%similarity, %max);
    my $z = new IO::Uncompress::Gunzip $inPath ||
        $self->logcroak("gunzip failed: $GunzipError");
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
    # convenience method to read metric thresholds from JSON config
    my ($self, $configPath) = @_;
    my %config = %{decode_json(read_file($configPath))};
    my %thresholds = %{$config{'Metrics_thresholds'}};
    return \%thresholds;
}

sub resultsCallRate {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'call_rate'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for call rate '",
                        $inPath, "' does not exist");
    }
    my $index = 1;
    return $self->resultsSpaceDelimited($inPath, $index);
}

sub resultsDuplicate {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'duplicate'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for duplicates '",
                        $inPath, "' does not exist");
    }
    my ($simRef, $maxRef) = $self->readDuplicates($inPath);
    my @subsets = $self->duplicateSubsets($simRef);
    my %max = %{$maxRef};
    # read call rates and find keep/discard status
    my %cr = %{$self->resultsCallRate()};
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
    if (defined $self->duplicate_subsets_path) {
        my %output;
        $output{$DUPLICATE_SUBSETS_KEY} = \@subsets;
        $output{$DUPLICATE_RESULTS_KEY} = \%results;
        open my $out, ">", $self->duplicate_subsets_path ||
            $self->logcroak("Cannot open output '",
                            self->duplicate_subsets_path, "'");
        print $out to_json(\%output);
        close $out ||
            $self->logcroak("Cannot close output '",
                            self->duplicate_subsets_path, "'");

    }
    return \%results;
}

sub resultsGender {
    # read gender results from sample_xhet_gender.txt
    # 'metric value' is concatenation of inferred, supplied gender codes
    # $threshold not used
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'gender'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for gender '",
                        $inPath, "' does not exist");
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
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'heterozygosity'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for heterozygosity '",
                        $inPath, "' does not exist");
    }
    my $index = 2;
    return $self->resultsSpaceDelimited($inPath, $index);
}

sub resultsHighMafHet {
    my ($self, ) = @_;
    return $self->resultsMafHet(1);
}

sub resultsIdentity {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'identity'};
    my $resultsRef;
    if (-e $inPath) {
        # read identity results from JSON file
        my %data = %{decode_json(read_file($inPath))};
        my @sample_results = @{$data{'identity'}};
        my %results;
        foreach my $result (@sample_results) {
            my $name = $result->{'sample_name'};
            my $concordance = $result->{'concordance'};
            my $identity = $result->{'identity'};
            $results{$name} = [$identity, $concordance];
        }
        $resultsRef = \%results;
    } else {
        $self->info("Omitting identity metric; expected identity JSON path '",
                    $inPath, "' does not exist");
    }
    return $resultsRef;
}

sub resultsLowMafHet {
    my ($self, ) = @_;
    return $self->resultsMafHet(0);
}

sub resultsMafHet {
    # read JSON file output by Plinktools het_by_maf.py
    my ($self, $high) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'het_by_maf'};
    if (!(-r $inPath)) {
        $self->info("Omitting MAF heterozygosity; cannot read input '",
                    $inPath, "'");
        return 0;
    }
    my %data = %{decode_json(read_file($inPath))};
    my %results;
    foreach my $sample (keys(%data)) {
        # TODO modify output format of het_by_maf.py
        if ($high) { $results{$sample} = $data{$sample}{'high_maf_het'}[1]; }
        else { $results{$sample} = $data{$sample}{'low_maf_het'}[1]; }
    }
    return \%results;
}

sub resultsMagnitude {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'magnitude'};
    if (!(-e $inPath)) {
        $self->info("Omitting magnitude; input '", $inPath,
                    "' does not exist");
        return 0; # magnitude of intensity is optional
    }
    my $index = 1;
    return $self->resultsSpaceDelimited($inPath, $index);
}

sub resultsSpaceDelimited {
    # read metric results from a space-delimited file ()
    # TODO use Text::CSV instead?
    my ($self, $inPath, $index) = @_;
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
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'xydiff'};
    if (!(-e $inPath)) { 
        $self->info("Omitting xydiff; input '", $inPath,
                    "' does not exist");
        return 0;
    }
    my $index = 1;
    return $self->resultsSpaceDelimited($inPath, $index);
}

sub transposeResults {
    # convert results from metric-major to sample-major ordering
    my ($self, $resultsRef) = @_;
    my %metricResults = %{$resultsRef};
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
    my ($self, $passRef) = @_;
    my %samplePass = %{$passRef};
    # samples which were previously excluded should *remain* excluded

    $self->db->connect(RaiseError => 1,
                       on_connect_do => 'PRAGMA foreign_keys = ON');
    my @samples = $self->db->sample->all;
    $self->db->in_transaction(sub {
                                  foreach my $sample (@samples) {
                                      my $uri = $sample->uri;
                                      if (!($samplePass{$uri})) {
                                          $sample->update({'include' => 0});
                                      }
                                  }
                              });
    $self->db->disconnect();
}

sub writeCsv {
    # write a .csv file summarizing metrics and pass/fail status
    my $self = shift; # TODO fix argument parsing
    my $csvPath = shift;
    my %passResult = %{ shift() }; # metric pass/fail status and values
    my %samplePass = %{ shift() }; # overall pass/fail by sample


    my %sampleInfo = $self->dbSampleInfo();     # generic sample/dataset info
    my @excluded =  $self->dbExcludedSamples(); # samples excluded in DB
    my %excluded;
    foreach my $sample (@excluded) { $excluded{$sample} = 1; }
    my @lines = ();

    my @sampleNames = keys(%sampleInfo);
    my $bySampleName = $self->_getBySampleName();
    @sampleNames = sort $bySampleName @sampleNames;

    my ($linesRef, $metricsRef);
    # first pass; append lines for samples included in pipeline DB
    ($linesRef, $metricsRef) =
        $self->includedSampleCsv(\@sampleNames, \%sampleInfo,
                                 \%passResult, \%samplePass, \%excluded);
    push(@lines, @{$linesRef});
    # second pass; append dummy lines for excluded samples
    $linesRef = $self->excludedSampleCsv(\@sampleNames, \%sampleInfo,
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
        } elsif ($name eq $ID_NAME) {
            @suffixes = qw/pass probability concordance/;
        } else {
            @suffixes = qw/pass value/;
        }
        foreach my $suffix (@suffixes) { push(@headers, $name.'_'.$suffix); }
    }
    unshift(@lines, join(',', @headers));
    # write results to file
    open my $out, ">", $csvPath ||
        $self->logcroak("Cannot open output '$csvPath'");
    foreach my $line (@lines) { print $out $line."\n"; }
    close $out || $self->logcroak("Cannot close output '$csvPath'");
}

sub writeJson {
    # open a file, and write the given reference in JSON format
    my ($self, $outPath, $dataRef) = @_;
    my $resultString = encode_json($dataRef);
    open my $out, ">", $outPath ||
        $self->logcroak("Cannot open output path '$outPath'");
    print $out $resultString;
    close($out) || $self->logcroak("Cannot close output path '$outPath'");
    return 1;
}

sub collate {
    # main method to collate results and write outputs
    # $metricsRef is an optional reference to an array of metric names; use to specify a subset of metrics for evaluation
    my ($self, $statusJson, $metricsJson, $csvPath, $exclude) = @_;
    my (%config, %thresholdConfig);
    $self->debug("Started collating QC results for input ", $self->input_dir);

    # 0) reprocess duplicate results for given threshold (if any)
    my $duplicate_param = $self->threshold_parameters->{$DUP_NAME};
    if (defined $duplicate_param) {
        $self->processDuplicates($duplicate_param);
    }

    # 1) find metric values (and write to file if required)
    my $metricResultsRef = $self->findMetricResults();
    my $sampleResultsRef = $self->transposeResults($metricResultsRef);
    $self->debug("Found metric values.");
    if ($metricsJson) { $self->writeJson($metricsJson, $sampleResultsRef);  }
    if ($statusJson || $csvPath || $exclude) {
        # if output options require evaluation of thresholds
        # 2) apply filters to find pass/fail status
        my $thresholds = $self->findThresholds($metricResultsRef);
        my $passResultRef = $self->evaluateThresholds($sampleResultsRef,
                                                      $thresholds);
        $self->debug("Evaluated pass/fail status.");

        # 3) add location info and write JSON status file
        $passResultRef = $self->addLocations($passResultRef);
        $self->writeJson($statusJson, $passResultRef);
        $self->debug("Wrote status JSON file $statusJson.");

        # 4) write CSV (if required)
        my %samplePass = $self->passFailBySample($passResultRef);
        if ($csvPath) {
            $self->writeCsv($csvPath, $passResultRef, \%samplePass);
            $self->debug("Wrote CSV $csvPath.");
        }

        # 5) exclude failing samples in pipeline DB (if required)
        if ($exclude) { $self->updateDatabase(\%samplePass); }
        $self->debug("Updated pipeline DB.");
    }
}

sub _build_db {
    my ($self,) = @_;
    my $db = WTSI::NPG::Genotyping::Database::Pipeline->new
	(name    => 'pipeline',
	 inifile => $self->ini_path,
	 dbfile  => $self->db_path);
    return $db;
}

sub _build_filenames {
    my ($self,) = @_;
    my $config = decode_json(read_file($self->config_path));
    return $config->{'collation_names'};
}


sub _build_metrics {
    my ($self,) = @_;
    my @metrics = keys %{$self->threshold_parameters};
    @metrics = sort @metrics;
    return \@metrics;
}

sub _build_threshold_parameters {
    my ($self,) = @_;
    my %thresholds;
    my $input_path;
    if (defined($self->filter_path)) {
        $input_path = $self->filter_path;
    } else {
        $input_path = $self->config_path;
    }
    my $config = decode_json(read_file($input_path));
    return $config->{'Metrics_thresholds'};
}

sub _getBySampleName {
    my ($self,) = @_;
    # need a coderef to sort sample identifiers in writeCsv
    # wrapped in its own object method to satisfy Moose syntax & PerlCritic
    return sub {
        # comparison function for sorting samples
        # if in plate_well_id format, sort by id; otherwise use standard sort
        if ($a =~ m{[[:alnum:]]+_[[:alnum:]]+_[[:alnum:]]+}msx &&
                $b =~ m{[[:alnum:]]+_[[:alnum:]]+_[[:alnum:]]+}msx) {
            my @termsA = split /_/msx, $a;
            my @termsB = split /_/msx, $b;
            return $termsA[-1] cmp $termsB[-1];
        } else {
            return $a cmp $b;
        }
    }
}

no Moose;

1;
