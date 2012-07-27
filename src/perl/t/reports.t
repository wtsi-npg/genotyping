# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Test::More tests => 6;
use JSON;
use WTSI::Genotyping::QC::Reports;

my $resultPath = "$Bin/qc/alpha/qc_results.json";
my $dbPath = "$Bin/qc_test_data/alpha_pipeline.db";
my $textPath = "$Bin/qc/alpha/pipeline_summary.txt";
my $csvPath = "$Bin/qc/alpha/pipeline_summary.csv";

my @text = textForDatasets($dbPath);
foreach my $ref (@text) { print join(",", @$ref)."\n"; }
ok(@text, "Read database dataset info");

my $result = dbSampleInfo($dbPath);
ok($result, "Read database sample info");

my @plateText = textForPlates($resultPath);
is(@plateText, 13, "Find plate table text"); # expect exactly 13 lines, including header

my @csvText = textForCsv($resultPath, $dbPath);
is(@csvText, 996, "Find CSV text"); # expect 996 lines, including header

ok(writeSummaryText($resultPath, $dbPath, $textPath), "Write summary text");

ok(writeCsv($resultPath, $dbPath, $csvPath), "Write CSV text"); 
