#! /usr/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012
# test for refactored check_duplicates_bed script

use strict;
use warnings;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotTests;

my $inputDir = "/nfs/gapi/users/ib5/genotype_qc_test_data/duplicate_test/illuminus1/";
my $outputDirName = "check_duplicates_output";
my $prefix = WTSI::Genotyping::QC::QCPlotTests::readPrefix($inputDir."/../../prefix.txt");
my $cmd = "perl ${Bin}/../bin/check_duplicates_bed.pl ".$prefix;

chdir($inputDir);
my ($tests, $failures) = (0,0);
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
my $output = "duplicate_full.txt";
my $refDir = "${Bin}/${outputDirName}/";
my @cols = (3,4);
my $pattern = "*.txt";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::diffGlobs($refDir, ".", \*STDOUT, $tests, $failures, 
								   $pattern, \@cols);
@cols = ();
$pattern = "*.log";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::diffGlobs($refDir, ".", \*STDOUT, $tests, $failures, 
								   $pattern, \@cols);
