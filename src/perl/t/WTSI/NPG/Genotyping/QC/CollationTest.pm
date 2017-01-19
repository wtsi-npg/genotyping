use utf8;

package WTSI::NPG::Genotyping::QC::CollationTest;

use strict;
use warnings;

use File::Copy qw/copy/;
use File::Slurp qw/read_file/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw/tempdir/;
use FindBin qw/$Bin/;
use JSON;
use Text::CSV;

use base qw(WTSI::NPG::Test);
use Test::More tests => 16;
use Test::Exception;

use WTSI::NPG::Genotyping::QC::Collation;
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
my $csvExpected = catfile($example_dir, 'qc_results.csv');

my $expectedCsvContents;

sub make_fixture : Test(setup) {
    $temp_dir = tempdir("CollationTest_XXXXXX", CLEANUP => 1);
    open my $fh, "<", $csvExpected || $log->logcroak("Cannot open CSV '",
                                                     $csvExpected, "'");
    my $csv = Text::CSV->new();
    $expectedCsvContents = $csv->getline_all($fh);
    close $fh || $log->logcroak("Cannot close CSV '", $csvExpected, "'");
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC::Collation');
}

sub collation : Test(6) {
    my $jsonResults = catfile($temp_dir, 'qc_results.json');
    my $jsonMetrics = catfile($temp_dir, 'qc_metrics.json');
    my $csvPath = catfile($temp_dir, 'qc_results.csv');
    my $exclude = 0;
    my $metricsRef = 0;
    my $verbose = 0;
    my $collator = WTSI::NPG::Genotyping::QC::Collation->new(
        db_path  => $dbPath,
        ini_path => $iniPath
    );
    $collator->collate($example_dir, $configPath, $thresholdPath,
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
    is_deeply($csvContents, $expectedCsvContents, "CSV contents match");
}

sub collation_script : Test(8) {
    my $jsonResults = catfile($temp_dir, 'qc_results.json');
    my $jsonMetrics = catfile($temp_dir, 'qc_metrics.json');
    my $csvPath = catfile($temp_dir, 'qc_results.csv');
    # apply sample exclusion to temporary copy of DB
    my $dbTemp = catfile($temp_dir, 'genotyping.db');
    copy($dbPath, $dbTemp) || $log->logcroak("Failed to copy database from ",
                                             $dbPath, " to ", $dbTemp);
    ok(system(join q{ }, "collate_qc_results.pl",
              "--input $example_dir",
              "--status $jsonResults",
              "--dbpath $dbTemp",
              "--csv $csvPath",
              "--metrics $jsonMetrics",
              "--config $configPath",
              "--exclude") == 0, 'Ran collation script');
    # check for sample exclusion in database
    my $exclPath = catfile($example_dir, "qc_exclusions.json");
    my $expectedInclusion = decode_json(read_file($exclPath));
    my $result = `echo 'select name,include from sample;' | sqlite3 $dbTemp`;
    my @lines = split("\n", $result);
    my %inclusion;
    foreach my $line (@lines) {
	my @fields = split('\|', $line);
	my $status = pop @fields;
	my $name = join("|", @fields); # OK even if name includes | characters
	$inclusion{$name} = $status;
    }
    is_deeply(\%inclusion, $expectedInclusion,
              "Sample inclusion status in pipeline DB");
    # check other outputs
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
    is_deeply($csvContents, $expectedCsvContents, "CSV contents match");
}

1;
