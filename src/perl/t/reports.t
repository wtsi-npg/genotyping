# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Test::More tests => 10;
use JSON;
use WTSI::Genotyping::QC::Reports qw(createReports);
use WTSI::Genotyping::QC::QCPlotShared qw/defaultJsonConfig/;

my $resultPath = "$Bin/qc/alpha/qc_results.json";
my $dbPath = "$Bin/qc_test_data/alpha_pipeline.db";
my $gtPath = "$Bin/qc/alpha/sample_xhet_gender_thresholds.txt";
my $texPath = "$Bin/qc/alpha/pipeline_summary.tex";
my $pdfPath = "$Bin/qc/alpha/pipeline_summary.pdf";
my $csvPath = "$Bin/qc/alpha/pipeline_summary.csv";
my $qcDir = "$Bin/qc/alpha/";
my $qcName = "alpha";
my $title = "";
my $author = "";
my $introPath = "$Bin/../etc/reportIntro.tex";
my $config = defaultJsonConfig();

my @text = WTSI::Genotyping::QC::Reports::textForDatasets($dbPath);
ok(@text, "Read database dataset info");

my $result = WTSI::Genotyping::QC::Reports::dbSampleInfo($dbPath);
ok($result, "Read database sample info");

my ($keyRef, $countRef, $rateRef) = 
    WTSI::Genotyping::QC::Reports::textForPlates($resultPath, $config);
# expect exactly 13 lines, including header
is(@{$countRef}, 13, "Find pass/fail count table text"); 
is(@{$rateRef}, 13, "Find pass/fail rate table text"); 

my @csvText = WTSI::Genotyping::QC::Reports::textForCsv($resultPath, $dbPath, 
                                                        $config);
is(@csvText, 996, "Find CSV text"); # expect 996 lines, including header

ok(WTSI::Genotyping::QC::Reports::writeCsv($resultPath, $dbPath, $config, 
                                           $csvPath), "Write CSV text"); 

ok(WTSI::Genotyping::QC::Reports::writeSummaryLatex
   ($texPath, $resultPath, $config, $dbPath, $gtPath, $qcDir, $introPath, 
    $qcName, $title, $author), "Write summary .tex");

if (-e $pdfPath) { system("rm -f $pdfPath"); }

ok(WTSI::Genotyping::QC::Reports::texToPdf($texPath), "Convert .tex to .pdf");

ok((-e $pdfPath), "PDF file exists");

ok(createReports($csvPath, $texPath, $resultPath, $config, $dbPath, $gtPath, 
                 $qcDir, $introPath, $qcName, $title, $author), 
   "Main method to create CSV and PDF reports");

