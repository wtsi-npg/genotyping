# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use Test::More tests => 11;
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
my $gtName = "sample_xhet_gender_thresholds.txt";
my $tempdir = tempdir("/tmp/qc_report_tXXXXXX", CLEANUP => 1);
my $cmd = "cp $inputdir$dbName $inputdir$resultName $inputdir$gtName ".
    "${inputdir}*.pdf $tempdir";
if (system($cmd)!=0) { 
    croak "Cannot copy input files to temporary directory!"; 
}
my $dbPath = $tempdir."/".$dbName;
my $resultPath = $tempdir."/".$resultName;
my $gtPath = $tempdir."/".$gtName;
my $prefix = $tempdir."/report_test_pipeline_summary";
my $texPath = $prefix.".tex";
my $pdfPath = $prefix.".pdf";
my $csvPath = $prefix.".csv";

my @text = WTSI::NPG::Genotyping::QC::Reports::textForDatasets($dbPath);
ok(@text, "Read database dataset info");

my $result = WTSI::NPG::Genotyping::QC::Reports::dbSampleInfo($dbPath);
ok($result, "Read database sample info");

my ($sumRef, $keyRef, $countRef, $rateRef) = 
    WTSI::NPG::Genotyping::QC::Reports::textForPlates($resultPath, $config);
# expect exactly 13 lines, including header
is(@{$countRef}, 13, "Find pass/fail count table text"); 
is(@{$rateRef}, 13, "Find pass/fail rate table text"); 

my @csvText = WTSI::NPG::Genotyping::QC::Reports::textForCsv($resultPath, $dbPath, 
                                                        $config);
is(@csvText, 996, "Find CSV text"); # expect 996 lines, including header

ok(WTSI::NPG::Genotyping::QC::Reports::writeCsv($resultPath, $dbPath, $config, 
                                           $csvPath), "Write CSV text"); 

ok(checkCsv($csvPath, 996, 26), "Correct row/column totals in .csv file");

ok(WTSI::NPG::Genotyping::QC::Reports::writeSummaryLatex
   ($texPath, $resultPath, $config, $dbPath, $gtPath, $tempdir, $introPath, 
    $qcName, $title, $author), "Write summary .tex");

if (-e $pdfPath) { system("rm -f $pdfPath"); }

ok(WTSI::NPG::Genotyping::QC::Reports::texToPdf($texPath), "Convert .tex to .pdf");

ok((-e $pdfPath), "PDF file exists");

ok(createReports($csvPath, $texPath, $resultPath, $config, $dbPath, $gtPath, 
                 $tempdir, $introPath, $qcName, $title, $author), 
   "Main method to create CSV and PDF reports");


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
