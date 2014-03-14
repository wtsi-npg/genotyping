# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use Test::More tests => 7;
use JSON;
use WTSI::NPG::Genotyping::QC::Reports qw(createReports);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/defaultJsonConfig/;

my $qcName = "alpha";
my $title = "";
my $author = "";
my $introPath = "$Bin/../etc/reportIntro.tex";
my $config = defaultJsonConfig();

my $inputdir = "$Bin/reports/";
my $dbName = "test_pipeline.db";
my $resultName = "qc_results.json";
my $idName = 'identity_check.json';
my $gtName = "sample_xhet_gender_thresholds.txt";
my $tempdir = tempdir("/tmp/qc_report_tXXXXXX", CLEANUP => 1);
my $cmd = "cp $inputdir$dbName $inputdir$resultName $inputdir$gtName ".
    "$inputdir$idName ${inputdir}*.pdf $tempdir";
if (system($cmd)!=0) { 
    croak "Cannot copy input files to temporary directory!"; 
}
my $dbPath = $tempdir."/".$dbName;
my $resultPath = $tempdir."/".$resultName;
my $idPath = $tempdir."/".$idName;
my $gtPath = $tempdir."/".$gtName;
my $prefix = $tempdir."/report_test_pipeline_summary";
my $texPath = $prefix.".tex";
my $pdfPath = $prefix.".pdf";

my @text = WTSI::NPG::Genotyping::QC::Reports::textForDatasets($dbPath);
ok(@text, "Read database dataset info");

my ($sumRef, $keyRef, $countRef, $rateRef) = 
    WTSI::NPG::Genotyping::QC::Reports::textForPlates($resultPath, $config);
# expect exactly 13 lines, including header
is(@{$countRef}, 13, "Find pass/fail count table text"); 
is(@{$rateRef}, 13, "Find pass/fail rate table text"); 

ok(WTSI::NPG::Genotyping::QC::Reports::writeSummaryLatex
   ($texPath, $resultPath, $idPath, $config, $dbPath, $gtPath, $tempdir, 
    $introPath, $qcName, $title, $author), "Write summary .tex");

if (-e $pdfPath) { system("rm -f $pdfPath"); }

ok(WTSI::NPG::Genotyping::QC::Reports::texToPdf($texPath), "Convert .tex to .pdf");

ok((-e $pdfPath), "PDF file exists");

ok(createReports($texPath, $resultPath, $idPath, $config, $dbPath, $gtPath, 
                 $tempdir, $introPath, $qcName, $title, $author), 
   "Main method to create CSV and PDF reports");

