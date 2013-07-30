# Author:  Iain Bancarz, ib5@sanger.ac.uk
# November 2012

# Flag samples for exclusion in pipeline database, based on metric values
# Use to exclude samples with low Gencall CR from Illuminus input
# Also use to exclude samples failing QC from zCall

package WTSI::NPG::Genotyping::QC::MetricExclusion;

use strict;
use warnings;
use Carp;
use JSON;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/decode_json getDatabaseObject 
    meanSd readMetricResultHash parseThresholds readFileToString/;
use Log::Log4perl qw(:easy);
use Exporter;

Log::Log4perl->easy_init($ERROR);

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/runFilter/;

sub applyThresholds {
    # use given min/max thresholds to check if samples passed
    # IMPORTANT: gender and identity pass/fail status is retained
    my %min = %{shift()};
    my %max = %{shift()};
    my @metrics = @{shift()};
    my %results = %{shift()};
    my $logPath = shift;
    my %samplePass;
    my %metricPass;
    my %fail;
    my $failTotal = 0;
    my %warn = {};
    foreach my $metric (@metrics) { $fail{$metric}=0; }
    foreach my $uri (keys %results) {
        my $sampleOK = 1;
        unless (defined($results{$uri})) {
            croak "Result not found for URI \"$uri\"";
        }
        foreach my $metric (@metrics) {
            unless (defined($results{$uri}{$metric}) || $warn{$metric}) {
                carp "Result not found for metric \"$metric\"";
                $warn{$metric} = 1; # warn on first occurrence only
                $metricPass{$uri}{$metric} = 1;
                next;
            }
            my ($oldPass, $value) = @{$results{$uri}{$metric}};
            my $newPass = 1;
            if ($metric eq 'gender' || $metric eq 'identity' || 
                $metric eq 'duplicate') {
                $newPass = $oldPass;
            } elsif (defined($min{$metric}) && $value < $min{$metric}) {
                $newPass = 0;
            } elsif (defined($max{$metric}) && $value > $max{$metric}) {
                $newPass = 0;
            }
            if ($newPass==0) { $sampleOK = 0; $fail{$metric}++; }
            $metricPass{$uri}{$metric} = $newPass;
        }
        if (!$sampleOK) { $failTotal++; }
        $samplePass{$uri} = $sampleOK;
    }
    if ($logPath) {
        open my $log, ">", $logPath || croak "Cannot open log path $logPath";
        foreach my $metric (@metrics) { 
            print $log "Failed:$metric\t$fail{$metric}\n"; 
        }
        my $sampleTotal = keys(%results);
        print $log "Total_failures\t$failTotal\n";
        print $log "Total_samples\t$sampleTotal\n";
        print $log "Pass_rate\t".(1 - $failTotal/$sampleTotal)."\n";
        close $log || croak "Cannot close log path $logPath";
    }
    return (\%samplePass, \%metricPass);
}

sub generateThresholds {
    # parse thresholds for exclusion from a .json file
    # thresholds may be simple min/max, or a standard deviation
    # if standard deviation, need to use data to find min/max
    my %config = %{shift()};
    my %results = %{shift()};
    my %thresholds = parseThresholds(%config);
    my @metrics = @{$config{'name_array'}};
    my %types = %{$config{'Threshold_types'}};
    my (%min, %max);
    foreach my $metric (@metrics) {
        # find minimum/maximum for each metric
        if ($types{$metric} eq 'Standard deviations') {
            my @minmax = sigmaMinMax(\%results, $metric, $thresholds{$metric});
            ($min{$metric}, $max{$metric}) = @minmax;
        } elsif ($types{$metric} eq 'Minimum') {
            $min{$metric} = $thresholds{$metric};
        } elsif ($types{$metric} eq 'Maximum') {
            $max{$metric} = $thresholds{$metric};
        }
    }
    return (\%min, \%max, \@metrics);
}

sub runFilter {
    # 'main' method to run prefilter for Illuminus/zCall
    # Filter workflow:
    # 1. read thresholds and types from JSON file 
    # 2. read sample metrics from JSON file
    # 3. compute standard deviations where required
    # 4. apply min/max thresholds to identify samples as pass/fail
    # 5. exclude failed samples from database
    my %config = %{decode_json(readFileToString(shift()))};
    my %results = %{decode_json(readFileToString(shift()))};
    if (keys(%results)==0) { croak "Sample results input is empty"; }
    my $dbPath = shift;
    my $outPath = shift;
    my $logPath = shift;
    my ($minRef, $maxRef, $metricsRef) = generateThresholds(\%config,\%results);
    my ($spRef, $mpRef) = applyThresholds($minRef, $maxRef, $metricsRef, 
                                          \%results, $logPath);
    updateDatabase($dbPath, $spRef);
    open my $out, ">", $outPath || croak "Cannot open output $outPath";
    print $out encode_json($mpRef);
    close $out || croak "Cannot close output $outPath";
    
}

sub sigmaMinMax {
    # find min/max given data, metric name, and number of standard deviations
    # assumes symmetric thresholds, ie. same number of sd's above/below mean
    my ($resultsRef, $metric, $sdMax) = @_;
    my %results = %{$resultsRef};
    my @values;
    foreach my $uri (keys %results) {
        if (!defined($results{$uri}{$metric})) {
            croak "No results for sample $uri, metric $metric";
        }
        my ($pass, $value) = @{$results{$uri}{$metric}};
        push @values, $value;
    }
    my ($mean, $sd) = meanSd(@values);
    my $min = $mean - $sdMax * $sd;
    my $max = $mean + $sdMax * $sd;
    return ($min, $max);
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

return 1;
