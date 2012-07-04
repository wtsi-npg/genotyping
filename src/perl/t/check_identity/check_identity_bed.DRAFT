#! /usr/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012
# test for refactored check_duplicates_bed script

use strict;
use warnings;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotTests;

my $inputDir = "/nfs/gapi/users/ib5/genotype_qc_test_data/identity_test/illuminus1/";
my $outputDirName = "check_identity_output";
my $prefix = WTSI::Genotyping::QC::QCPlotTests::readPrefix($inputDir."/../../prefix.txt");
my $cmd = "perl ${Bin}/../bin/check_identity_bed.pl ".$prefix;
chdir($inputDir);
my ($tests, $failures) = (0,0);
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
# run diff on reference and output files
my $refDir = $Bin.'/'.$outputDirName;
my @cols = (1,2,3,4);
my $pattern = "identity_check_results.txt";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::diffGlobs($refDir, ".", \*STDOUT, $tests, $failures, 
								   $pattern, \@cols);
@cols = (2,3);
$pattern = "identity_check_gt.txt";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::diffGlobs($refDir, ".", \*STDOUT, $tests, $failures, 
								   $pattern, \@cols);
@cols = ();
$pattern = "identity_check_fail*";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::diffGlobs($refDir, ".", \*STDOUT, $tests, $failures, 
								   $pattern, \@cols);
