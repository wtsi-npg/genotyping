
package WTSI::NPG::Genotyping::QC::Collator;

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# February 2014; substantially refactored, January 2017

# Collate QC result outputs into a single JSON summary file
# Apply thresholds to find pass/fail status

use strict;
use warnings;
use File::Slurp qw(read_file);
use IO::Uncompress::Gunzip qw($GunzipError); # for duplicate_full.txt.gz
use JSON;
use Moose;
use Text::CSV;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(meanSd);

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
our @GENDERS = ('Unknown', 'Male', 'Female', 'Not_Available');
our $DUPLICATE_SUBSETS_KEY = 'SUBSETS';
our $DUPLICATE_RESULTS_KEY = 'RESULTS';
our $UNKNOWN_PLATE = "Unknown_plate";
our $UNKNOWN_ADDRESS = "Unknown_address";

######################################################################
### attributes supplied as init_args ###

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

######################################################################
### attributes not in init_args ###

has 'db'  =>
  (is         => 'ro',
   isa        => 'WTSI::NPG::Genotyping::Database::Pipeline',
   lazy       => 1,
   init_arg   => undef,
   builder    => '_build_db');

has 'duplicate_similarity' =>
  (is         => 'ro',
   isa        => 'HashRef[HashRef]',
   lazy       => 1,
   builder    => '_build_duplicate_similarity',
   documentation => 'Subsets of samples with mutual similarity above a '.
       'given threshold. The subset member with the highest call rate '.
       'passes the duplicate check; others fail.'
);

has 'duplicate_subsets' =>
  (is         => 'ro',
   isa        => 'ArrayRef[ArrayRef]',
   lazy       => 1,
   init_arg   => undef,
   builder    => '_build_duplicate_subsets',
   documentation => 'Subsets of samples with mutual similarity above a '.
       'given threshold. The subset member with the highest call rate '.
       'passes the duplicate check; others fail.'
);

has 'filenames' =>
  (is         => 'ro',
   isa        => 'HashRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_filenames');

has 'metric_names' =>
  (is         => 'ro',
   isa        => 'ArrayRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_metric_names');

has 'metric_results' =>
  (is         => 'ro',
   #isa        => 'HashRef[ArrayRef]', # TODO store ArrayRef for all metrics
   isa        => 'HashRef',
   lazy       => 1,
   init_arg   => undef,
   builder    => '_build_metric_results',
   documentation => 'For each metric, generate an ArrayRef with one or '.
       'more values to represent the metric outcome.'
);

has 'pass_fail_details' =>
  (is         => 'ro',
   isa        => 'HashRef[HashRef]',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_pass_fail_details',
   documentation => 'Pass/fail status for each sample/metric combination, '.
       'indexed by sample and metric',
);

has 'pass_fail_summary' =>
  (is         => 'ro',
   isa        => 'HashRef[Bool]',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_pass_fail_summary',
   documentation => 'Overall pass/fail status for each sample'
);

has 'thresholds' =>
  (is         => 'ro',
   isa        => 'HashRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_thresholds',
   documentation => 'Actual thresholds to determine pass/fail status, '.
       'which may depend on metric values (eg. if defined in terms of '.
       'standard deviations from the mean).');

has 'threshold_parameters' =>
  (is         => 'ro',
   isa        => 'HashRef',
   lazy       => 1,
   init_arg => undef,
   builder    => '_build_threshold_parameters',
   documentation => 'Parameters to determine pass/fail thresholds for '.
       'each metric.'
);

sub excludeFailedSamples {
    # if any samples have failed QC, set their 'include' value to False
    # samples which have not failed QC are unaffected
    my ($self, ) = @_;
    my %samplePass = %{$self->pass_fail_summary};
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

sub hasDuplicatesThreshold {
    # check if a threshold is defined for the duplicate metric
    my ($self, ) = @_;
    if (defined $self->thresholds->{$DUP_NAME}) {
        return 1;
    } else {
        return 0;
    }
}

sub writeCsv {
    my ($self, $outPath) = @_;
    my %passResult = %{$self->_add_locations($self->pass_fail_details)};
    my %samplePass = %{$self->pass_fail_summary};
    my %sampleInfo = $self->_db_sample_info();  # generic sample/dataset info
    my @excluded =  $self->_db_excluded_samples(); # samples excluded in DB
    my %excluded;
    foreach my $sample (@excluded) { $excluded{$sample} = 1; }
    my @lines = ();

    my @sampleNames = keys(%sampleInfo);
    my $bySampleName = $self->_getBySampleName();
    @sampleNames = sort $bySampleName @sampleNames;

    my ($linesRef, $metricsRef);
    # first pass; append lines for samples included in pipeline DB
    ($linesRef, $metricsRef) =
        $self->_included_sample_csv(\@sampleNames, \%sampleInfo,
                                    \%passResult, \%samplePass, \%excluded);
    push(@lines, @{$linesRef});
    # second pass; append dummy lines for excluded samples
    $linesRef = $self->_excluded_sample_csv(\@sampleNames, \%sampleInfo,
                                            $metricsRef, \%excluded);
    push(@lines, @{$linesRef});
    my %metrics = %{$metricsRef};
    # use %metrics to construct appropriate CSV header
    my @headers = qw/run project data_supplier snpset rowcol beadchip_number
                     supplier_name cohort sample include plate well pass/;
    foreach my $name (@{$self->metric_names}) {
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
    open my $out, ">", $outPath ||
        $self->logcroak("Cannot open output '$outPath'");
    foreach my $line (@lines) { print $out $line."\n"; }
    close $out || $self->logcroak("Cannot close output '$outPath'");
    return 1;
}

sub writeDuplicates {
    my ($self, $outPath) = @_;
    my %output;
    if ($self->hasDuplicatesThreshold()) {
        $output{$DUPLICATE_SUBSETS_KEY} = $self->duplicate_subsets;
        my $results = $self->metric_results->{$DUP_NAME};
        $output{$DUPLICATE_RESULTS_KEY} = $results;

        open my $out, ">", $outPath ||
            $self->logcroak("Cannot open output '", $outPath, "'");
        print $out to_json(\%output);
        close $out ||
            $self->logcroak("Cannot close output '", $outPath, "'");
        return 1;
    } else {
        $self->logwarn('No duplicate threshold defined; omitting ',
                       'duplicate output');
        return 0;
    }
}

sub writeMetricJson {
    my ($self, $outPath) = @_;
    my $sampleResultsRef = $self->_transpose_results($self->metric_results);
    $self->_write_json($outPath, $sampleResultsRef);
    return 1;
}

sub writePassFailJson {
    my ($self, $outPath) = @_;
    my $passResultRef = $self->_add_locations($self->pass_fail_details);
    $self->_write_json($outPath, $passResultRef);
    return 1;
}

sub _add_locations {
    # add plate/well locations to a hash indexed by sample
    my ($self, $samplesRef) = @_;
    my %samples = %{$samplesRef};
    $self->db->connect(RaiseError => 1,
                       on_connect_do => 'PRAGMA foreign_keys = ON');
    my %plateLocs;
    $self->db->in_transaction(
        sub {
            foreach my $sample ($self->db->sample->all) {
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
    foreach my $uri (keys %samples) {
        my %results = %{$samples{$uri}};
        if (defined($plateLocs{$uri})) {
            # samples with unknown location will have dummy values in hash
            my ($plate, $addressLabel) = @{$plateLocs{$uri}};
            $results{'plate'} = $plateLocs{$uri}->[0];
            $results{'address'} = $plateLocs{$uri}->[1];
            $samples{$uri} = \%results;
        } else {
            # excluded sample has *no* location value
            $self->logwarn('Excluded sample URI ', $uri,
                           'is in QC metric data');
        }
    }
    return \%samples;
}

sub _append_null {
    # append 'NA' values to given list
    my ($self, $arrayRef, $nullTotal) = @_;
    my @array = @{$arrayRef};
    for (my $i=0;$i<$nullTotal;$i++) {
        push(@array, 'NA');
    }
    return @array;
}

sub _build_db {
    my ($self,) = @_;
    my $db = WTSI::NPG::Genotyping::Database::Pipeline->new
	(name    => 'pipeline',
	 inifile => $self->ini_path,
	 dbfile  => $self->db_path);
    return $db;
}

sub _build_duplicate_similarity {
    my ($self,) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'duplicate'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for duplicates '",
                        $inPath, "' does not exist");
    }
    my %similarity;
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
    return \%similarity;
}

sub _build_duplicate_subsets {
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
    my ($self,) = @_;
    my %similarity = %{$self->duplicate_similarity};
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
    return \@subsets;
}

sub _build_filenames {
    my ($self,) = @_;
    my $config = decode_json(read_file($self->config_path));
    my $names = $config->{'collation_names'};
    if (! defined $names) {
        $self->logcroak("No collation_names entry in QC config file '",
                        $self->config_path, "'");
    }
    return $names;
}

sub _build_metric_names {
    my ($self,) = @_;
    my @metrics;
    my @ordered_metrics = ($ID_NAME, $DUP_NAME, $GENDER_NAME, $CR_NAME,
                           $HET_NAME, $LMH_NAME, $HMH_NAME, $MAG_NAME,
                           $XYD_NAME);
    foreach my $metric (@ordered_metrics) {
        if (defined $self->threshold_parameters->{$metric}) {
            push @metrics, $metric;
        }
    }
    return \@metrics;
}

sub _build_metric_results {
    # find QC results in metric-major order, return a hash reference
    # "results" for gender, duplicate, and identity are complex! represent as lists. For other metrics, result is a single float. See methods in write_qc_status.pl
    my ($self,) = @_;
    my %allResults;
    foreach my $name (@{$self->metric_names}) {
        my $resultsRef;
        if ($name eq $CR_NAME) {
            $resultsRef = $self->_results_call_rate();
        } elsif ($name eq $DUP_NAME) {
            $resultsRef = $self->_results_duplicate();
        } elsif ($name eq $GENDER_NAME) {
            $resultsRef = $self->_results_gender();
        } elsif ($name eq $HET_NAME) {
            $resultsRef = $self->_results_het();
        } elsif ($name eq $HMH_NAME) {
            $resultsRef = $self->_results_high_maf_het();
        } elsif ($name eq $ID_NAME) {
            $resultsRef = $self->_results_identity();
        } elsif ($name eq $LMH_NAME) {
            $resultsRef = $self->_results_low_maf_het();
        } elsif ($name eq $MAG_NAME) {
            $resultsRef = $self->_results_magnitude();
        } elsif ($name eq $XYD_NAME) {
            $resultsRef = $self->_results_xydiff();
        } else {
            $self->logcroak("Unknown metric name $name for results: $!");
        }
        if ($resultsRef) { $allResults{$name} = $resultsRef; }
    }
    return \%allResults;
}

sub _build_pass_fail_details {
    my ($self, ) = @_;
    my $results = $self->_transpose_results($self->metric_results);
    my %evaluated = ();
    foreach my $sample (keys(%{$results})) {
        foreach my $metric (keys(%{$results->{$sample}})) {
            my $value = $results->{$sample}->{$metric};
            my $threshold = $self->thresholds->{$metric};
            if (!defined($threshold)) {
                $self->logcroak("No threshold defined for metric '",
                                $metric, "'");
            }
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
                if ($value eq 'NA' || $probability > $threshold) {
                    $pass = 1;
                }
            } else {
                $self->logcroak("Unknown metric name '", $metric,
                                "' for pass/fail evaluation");
            }
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

sub _build_pass_fail_summary {
    my ($self,) = @_;
    my %results = %{$self->pass_fail_details};
    my %passFail = ();
    foreach my $sample (keys(%results)) {
        my %result = %{$results{$sample}};
        my $samplePass = 1;
        foreach my $metric (@{$self->metric_names}) {
            if (!defined($result{$metric})) { next; }
            my @values = @{$result{$metric}};
            my $pass = shift @values;
            if (!$pass) { $samplePass = 0; last; }
        }
        $passFail{$sample} = $samplePass;
    }
    return \%passFail;
}

sub _build_threshold_parameters {
    my ($self,) = @_;
    my $config = decode_json(read_file($self->config_path));
    my $thresholds = $config->{'Metrics_thresholds'};
    if (! defined $thresholds) {
        $self->logcroak("No Metrics_thresholds entry in QC config file '",
                        $self->config_path, "'");
    }
    return $thresholds;
}

sub _build_thresholds {
    # find threshold values, which may depend on mean/sd of metric values
    my ($self, ) = @_;
    my %metricResults = %{$self->metric_results};
    my %thresholds;
    my @names = keys(%metricResults);
    foreach my $metric (keys(%metricResults)) {
        if ($metric eq $HET_NAME || $metric eq $LMH_NAME ||
                $metric eq $HMH_NAME || $metric eq $XYD_NAME) {
            # find mean/sd for thresholds
            my %resultsBySample = %{$metricResults{$metric}};
            my ($mean, $sd) = meanSd(values(%resultsBySample));
            my $min = $mean - ($self->threshold_parameters->{$metric}*$sd);
            my $max = $mean + ($self->threshold_parameters->{$metric}*$sd);
            $thresholds{$metric} = [$min, $max];
        } elsif ($metric eq $CR_NAME || $metric eq $DUP_NAME ||
                     $metric eq $ID_NAME || $metric eq $GENDER_NAME ||
                     $metric eq $MAG_NAME ) {
            $thresholds{$metric} = $self->threshold_parameters->{$metric};
        } else {
            $self->logcroak("Unknown metric name '", $metric,
                            "' for thresholds");
        }
    }
    return \%thresholds;

}

sub _db_excluded_samples {
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

sub _db_sample_info {
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

sub _excluded_sample_csv {
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
        @fields = $self->_append_null(\@fields, 3); # null plate, well, pass
        foreach my $name (@{$self->metric_names}) {
            if (!$metrics{$name}) {
                next;
            } elsif ($name eq $GENDER_NAME) {
                # pass/fail, metric triple
                @fields = $self->_append_null(\@fields, 4);
            } elsif ($name eq $ID_NAME) {
                # pass/fail, metric double
                @fields = $self->_append_null(\@fields, 3);
            } else {
                # pass/fail, metric
                @fields = $self->_append_null(\@fields, 2);
            }
        }
        push(@lines, join(',', @fields));
    }
    return \@lines;
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

sub _included_sample_csv {
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
        foreach my $metric (@{$self->metric_names}) {
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

sub _read_tab_delimited_column {
    # read metric results from a tab-delimited file
    # omit any line starting with #
    # '$index' argument denotes the column with the desired metric values
    # assume first field in each line is the sample URI
    # return a hash of metric values indexed by sample URI
    my ($self, $inPath, $index) = @_;
    my @raw_lines = read_file($inPath);
    my @lines = grep { !/^[#]/msx } @raw_lines; # remove comments/headers
    my $csv = Text::CSV->new(
        { binary   => 1,
          sep_char => "\t",
      }
    );
    my %results;
    foreach my $line (@lines) {
        my $status = $csv->parse($line);
        if (! defined $status) {
            $self->logcroak("Unable to parse tab-delimited input line: '",
                            $line, "'");
        }
        my @fields = $csv->fields();
        my $uri = $fields[0];
        if (! defined $uri) {
            $self->logcroak("Unable to find URI from input '", $line, "'");
        }
        my $metric = $fields[$index];
        if (! defined $metric) {
            $self->logcroak("Unable to find field index ", $index,
                            " for line '", $line, "'");
        }
        $results{$uri} = $metric;
    }
    return \%results;
}

sub _results_call_rate {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'call_rate'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for call rate '",
                        $inPath, "' does not exist");
    }
    my $index = 1;
    return $self->_read_tab_delimited_column($inPath, $index);
}

sub _results_duplicate {
    my ($self, ) = @_;
    my %similarity = %{$self->duplicate_similarity};
    my %max;
    foreach my $sample_i (keys(%similarity)) {
        my $maxSim = 0;
        foreach my $sample_j (keys(%similarity)) {
            if ($sample_i eq $sample_j) { next; }
            my $sim = $similarity{$sample_i}{$sample_j};
            if ($sim > $maxSim) { $maxSim = $sim; }
        }
        $max{$sample_i} = $maxSim;
    }
    # read call rates and find keep/discard status
    my %cr = %{$self->_results_call_rate()};
    my %results;
    foreach my $subsetRef (@{$self->duplicate_subsets}) {
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
    return \%results;
}

sub _results_gender {
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

sub _results_het {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'heterozygosity'};
    if (!(-e $inPath)) {
        $self->logcroak("Input path for heterozygosity '",
                        $inPath, "' does not exist");
    }
    my $index = 2;
    return $self->_read_tab_delimited_column($inPath, $index);
}

sub _results_high_maf_het {
    my ($self, ) = @_;
    return $self->_results_maf_het(1);
}

sub _results_identity {
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

sub _results_low_maf_het {
    my ($self, ) = @_;
    return $self->_results_maf_het(0);
}

sub _results_maf_het {
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

sub _results_magnitude {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'magnitude'};
    if (!(-e $inPath)) {
        $self->info("Omitting magnitude; input '", $inPath,
                    "' does not exist");
        return 0; # magnitude of intensity is optional
    }
    my $index = 1;
    return $self->_read_tab_delimited_column($inPath, $index);
}

sub _results_xydiff {
    my ($self, ) = @_;
    my $inPath = $self->input_dir.'/'.$self->filenames->{'xydiff'};
    if (!(-e $inPath)) { 
        $self->info("Omitting xydiff; input '", $inPath,
                    "' does not exist");
        return 0;
    }
    my $index = 1;
    return $self->_read_tab_delimited_column($inPath, $index);
}

sub _transpose_results {
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

sub _write_json {
    # convenience method to write the given reference in JSON format
    my ($self, $outPath, $dataRef) = @_;
    my $resultString = encode_json($dataRef);
    open my $out, ">", $outPath ||
        $self->logcroak("Cannot open output path '$outPath'");
    print $out $resultString;
    close($out) || $self->logcroak("Cannot close output path '$outPath'");
    return 1;
}



no Moose;

1;


__END__

=head1 NAME

WTSI::NPG::Genotyping::QC::Collator - Class to collate QC metric outputs

=head1 SYNOPSIS

my $collator = WTSI::NPG::Genotyping::QC::Collator->new(
    db_path     => $dbPath,
    ini_path    => $iniPath,
    input_dir   => $inputDir,
    config_path => $config,
);

=head1 DESCRIPTION


Collate QC metric outputs from the WTSI genotyping pipeline, and perform
operations on the data. Operations include combining duplicate and call
rate data to evaluate the duplicate metric; assessment of pass/fail status
of samples; exclusion of failed samples in the pipeline SQLite database; and
output of QC result summaries in JSON or CSV format.

The JSON format output is used by downstream software to create QC plots
and reports.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015, 2016, 2017 Genome Research Limited.
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
