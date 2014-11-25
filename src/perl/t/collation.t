# Author:  Iain Bancarz, ib5@sanger.ac.uk
# February 2014

use strict;
use warnings;
use Carp;
use Digest::MD5;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use Test::More tests => 15;
use JSON;

use WTSI::NPG::Genotyping::QC::Collation qw(collate);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(readFileToString readSampleInclusion);

my $tempdir = tempdir("/tmp/qc_report_tXXXXXX", CLEANUP => 1);

my ($dir, $inputDir, $configPath, $dbPath, $iniPath, $jsonResults, 
    $jsonMetrics, $resultsMaster, $metricsMaster, $verbose,
    $csvPath, $exclude, $jsonMaster, $thresholdPath, $dbName, $md5, $fh);

$dbName = 'small_test.db';
$dir = "$Bin/qc_test_data/";
$inputDir = $dir.'output_examples/';
$configPath = $dir.'config_test.json';
$thresholdPath = $configPath;
$dbPath = $dir.$dbName;
$iniPath =  $ENV{HOME} . "/.npg/genotyping.ini";
$jsonResults = $tempdir.'/qc_results.json';
$jsonMetrics = $tempdir.'/qc_metrics.json';
$csvPath = $tempdir.'/qc_results.csv';
$exclude = 0;
$verbose = 0;
$resultsMaster = $inputDir.'/qc_results.json';
$metricsMaster = $inputDir.'/qc_metrics.json';

collate($inputDir, $configPath, $thresholdPath, $dbPath, $iniPath, 
	$jsonResults, $jsonMetrics, $csvPath, $exclude, 0, $verbose);
checkOutputs($jsonMetrics, $metricsMaster, $jsonResults, $resultsMaster, $csvPath);
system("rm -Rf $tempdir/*"); # remove output from previous tests
print "Removed output from previous tests; now testing main script.\n";
my $dbTemp = $tempdir.'/'.$dbName; # apply sample exclusion to temporary DB
system("cp $dbPath $dbTemp");
my $cmd = "collate_qc_results.pl --input $inputDir --status $jsonResults --dbpath $dbTemp --csv $csvPath --metrics $jsonMetrics --config $configPath --exclude"; 
is(0, system($cmd), 'Command-line script exit status OK');
checkOutputs($jsonMetrics, $metricsMaster, $jsonResults, $resultsMaster, $csvPath);

# check for sample exclusion in database
my $exclPath = $inputDir."/qc_exclusions.json";
my $expected = decode_json(readFileToString($exclPath));
my $excluded = readSampleInclusion($dbTemp);
is_deeply($excluded, $expected, "Sample inclusion status in pipeline DB");

# verify database checksum
$md5 = Digest::MD5->new;
open $fh, "<", $dbTemp || croak "Cannot open temporary DB $dbTemp";
binmode($fh);
while (<$fh>) { $md5->add($_); }
close $fh || croak "Cannot close temporary DB $dbTemp";
is($md5->hexdigest, '1088e683a09f281a62c4100ea7ff40ae',
   "MD5 checksum of DB after sample exclusion");

sub checkOutputs {
    # validate the expected collation output files
    # runs a total of 6 tests
    my ($jsonMetrics, $metricsMaster, $jsonResults, $resultsMaster, $csvPath) = @_;
    ok(-e $jsonMetrics, "JSON metrics path exists");
    my $output = decode_json(readFileToString($jsonMetrics));
    my $master = decode_json(readFileToString($metricsMaster));
    is_deeply($output, $master, "JSON metrics data equivalent to master copy");
    ok(-e $jsonResults, "JSON results path exists");
    $output = decode_json(readFileToString($jsonResults));
    $master = decode_json(readFileToString($resultsMaster));
    is_deeply($output, $master, "JSON results data equivalent to master copy");
    ok(-e $csvPath, "CSV results path exists");
    ok(checkCsv($csvPath, 101, 33), "Correct row/column totals in .csv file");
}


sub checkCsv {
    my ($inPath, $expectedRows, $expectedCols) = @_;
    my $rows = 0;
    my $ok = 1;
    open my $in, "<", $inPath || croak "Cannot open input $inPath"; 
    while (<$in>) {
        $rows++;
        chomp;
        my @fields = split(/,/);
        my $cols = scalar(@fields);
        if ($cols!=$expectedCols) { 
            print STDERR "Expected $expectedCols .csv columns, ".
                "found $cols at line $rows\n"; 
            $ok = 0; 
            last;
        } 
    }
    close $in || croak "Cannot close input $inPath";
    if ($ok==1 && $rows!=$expectedRows) { 
        print STDERR "Expected $expectedRows .csv rows, found $rows\n";
        $ok = 0; 
    }
    return $ok;
}
