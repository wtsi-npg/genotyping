# Author:  Iain Bancarz, ib5@sanger.ac.uk
# November 2012

# Flag samples for exclusion in pipeline database, based on metric values
# Use to exclude samples with low Gencall CR from Illuminus input

package WTSI::NPG::Genotyping::QC::MetricExclusion;

use strict;
use warnings;
use Carp;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/getDatabaseObject 
    readMetricResultHash/;
use Log::Log4perl qw(:easy);
use Exporter;

Log::Log4perl->easy_init($ERROR);

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/filterCR/;

sub filterCR {
    # filter on call rate
    my @args = @_;
    my $metric = 'call_rate';
    push(@args, $metric);
    filterMetricMinimum(@args);
}

sub filterMetricMinimum {
    # filter with given metric and minimum threshold
    # arguments: SQLite DB, .json config, .json results, threshold, metric name
    my ($dbPath, $configPath, $resultPath, $threshold, $metric) = @_;
    my %samplePass;
    my %metricResults = readMetricResultHash($resultPath, $configPath);
    foreach my $uri (keys(%metricResults)) {
        my %results = %{$metricResults{$uri}};
        my @result = @{$results{$metric}};
        if ($result[1] >= $threshold) {
            $samplePass{$uri} = 1;
        } else {
            $samplePass{$uri} = 0;
        }
    }
    # now use %samplePass to update database
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
