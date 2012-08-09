# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Test::More tests => 9;
use JSON;
use WTSI::Genotyping::QC::Reports;

my $resultPath = "$Bin/qc/alpha/qc_results.json";
my $dbPath = "$Bin/qc_test_data/alpha_pipeline.db";
my $texPath = "$Bin/qc/alpha/pipeline_summary.tex";
my $pdfPath = "$Bin/qc/alpha/pipeline_summary.pdf";
my $csvPath = "$Bin/qc/alpha/pipeline_summary.csv";
my $metricPath = "$Bin/../json/qc_threshold_defaults.json";
my $qcDir = "$Bin/qc/alpha/";
my $title = "Alpha";

my @text = WTSI::Genotyping::QC::Reports::textForDatasets($dbPath);
ok(@text, "Read database dataset info");

my $result = WTSI::Genotyping::QC::Reports::dbSampleInfo($dbPath);
ok($result, "Read database sample info");

my @plateText = WTSI::Genotyping::QC::Reports::textForPlates($resultPath);
is(@plateText, 13, "Find plate table text"); # expect exactly 13 lines, including header

my @csvText = WTSI::Genotyping::QC::Reports::textForCsv($resultPath, $dbPath);
is(@csvText, 996, "Find CSV text"); # expect 996 lines, including header

ok(WTSI::Genotyping::QC::Reports::writeCsv($resultPath, $dbPath, $csvPath), "Write CSV text"); 

ok(WTSI::Genotyping::QC::Reports::writeSummaryLatex($texPath, $resultPath, $metricPath, $dbPath, $qcDir, $title), 
   "Write summary .tex");

system("rm -f $pdfPath");

ok(WTSI::Genotyping::QC::Reports::texToPdf($texPath), "Convert .tex to .pdf");

ok((-e $pdfPath), "PDF file exists");

ok(createReports($resultPath, $dbPath, $csvPath, $texPath, $metricPath, $qcDir, $title), 
   "Main method to create CSV and PDF reports");

