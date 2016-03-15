use utf8;

package WTSI::NPG::Genotyping::QC::CollationTest;

use strict;
use warnings;

use File::Slurp qw/read_file/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw/tempdir/;
use FindBin qw/$Bin/;
use JSON;
use Text::CSV;

use base qw(WTSI::NPG::Test);
use Test::More tests => 109;
use Test::Exception;

use WTSI::NPG::Genotyping::QC::Collation qw(collate);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(readSampleInclusion);

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $log = Log::Log4perl->get_logger();

BEGIN { use_ok('WTSI::NPG::Genotyping::QC::Collation'); }

my $temp_dir;
my $dbName = 'small_test.db';
my $data_dir = "$Bin/qc_test_data/";
my $example_dir = catfile($data_dir, 'output_examples');

my $configPath = catfile($data_dir, 'config_test.json');
my $thresholdPath = $configPath;
my $dbPath = catfile($data_dir, 'small_test.db');
my $iniPath =  $ENV{HOME} . "/.npg/genotyping.ini";

my $resultsExpected = catfile($example_dir, 'qc_results.json');
my $metricsExpected = catfile($example_dir, 'qc_metrics.json');

sub make_fixture : Test(setup) {
    $temp_dir = tempdir(CLEANUP => 1);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC::Collation');
}


sub collation : Test(107) {

    my $jsonResults = catfile($temp_dir, 'qc_results.json');
    my $jsonMetrics = catfile($temp_dir, 'qc_metrics.json');
    my $csvPath = catfile($temp_dir, 'qc_results.csv');
    my $exclude = 0;
    my $metricsRef = 0;
    my $verbose = 0;

    collate($example_dir, $configPath, $thresholdPath, $dbPath, $iniPath,
            $jsonResults, $jsonMetrics, $csvPath,
            $exclude, $metricsRef, $verbose);
    ok(-e $jsonMetrics, "JSON metrics path exists");
    my $got_metrics = decode_json(read_file($jsonMetrics));
    my $expected_metrics = decode_json(read_file($metricsExpected));
    is_deeply($got_metrics, $expected_metrics,
              "JSON metrics data equivalent to expected");
    ok(-e $jsonResults, "JSON results path exists");
    my $got_results = decode_json(read_file($jsonResults));
    my $expected_results = decode_json(read_file($resultsExpected));
    is_deeply($got_results, $expected_results,
              "JSON results data equivalent to expected");
    ok(-e $csvPath, "CSV results path exists");
    open my $fh, "<", $csvPath || $log->logcroak("Cannot open CSV '",
                                                 $csvPath, "'");
    my $csv = Text::CSV->new();
    my $csvContents = $csv->getline_all($fh);
    close $fh || $log->logcroak("Cannot close CSV '", $csvPath, "'");
    is(scalar @{$csvContents}, 101, 'Correct row totals in .csv file');
    foreach my $row (@{$csvContents}) {
        is(scalar @{$row}, 33, 'Correct column totals in CSV row');
    }
}

#sub collation_excluded_sample : Test(1) {

#}
