# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Test::More tests => 8;
use JSON;
use WTSI::Genotyping::QC::Reports;

my $resultPath = "$Bin/qc/alpha/qc_results.json";
my $dbPath = "$Bin/qc_test_data/alpha_pipeline.db";
my $texPath = "$Bin/qc/alpha/pipeline_summary.tex";
my $pdfPath = "$Bin/qc/alpha/pipeline_summary.pdf";
my $csvPath = "$Bin/qc/alpha/pipeline_summary.csv";
my $metricPath = "$Bin/../json/qc_threshold_defaults.json";
my $qcDir = "$Bin/qc/alpha/";

my @text = textForDatasets($dbPath);
ok(@text, "Read database dataset info");

my $result = dbSampleInfo($dbPath);
ok($result, "Read database sample info");

my @plateText = textForPlates($resultPath);
is(@plateText, 13, "Find plate table text"); # expect exactly 13 lines, including header

my @csvText = textForCsv($resultPath, $dbPath);
is(@csvText, 996, "Find CSV text"); # expect 996 lines, including header

ok(writeCsv($resultPath, $dbPath, $csvPath), "Write CSV text"); 

ok(writeSummaryLatex($texPath, $resultPath, $metricPath, $dbPath, 0, 0, $qcDir), "Write summary .tex");

system("rm -f $pdfPath");

ok(texToPdf($texPath), "Convert .tex to .pdf");

ok((-e $pdfPath), "PDF file exists")
