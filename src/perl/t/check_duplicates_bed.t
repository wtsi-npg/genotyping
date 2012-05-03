#! /usr/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012
# test for refactored check_duplicates_bed script

use strict;
use warnings;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotTests;

my $inputDir = "/nfs/users/nfs_i/ib5/data/genotype_project_examples/Native_American_Population_Genetics_NAPG/illuminus1/"; # TODO find a better location for input test data; 16 MB PLINK files a little big to check into git
my $outputDirName = "check_duplicates_output";
my $prefix = "HumanOmni1-Quad_v1-0_B.bpm_2012-02-08_native_american_population_genetics_napg";
my $cmd = "perl ${Bin}/../bin/check_duplicates_bed.pl ".$prefix;

chdir($inputDir);
my ($tests, $failures) = (0,0);
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
my $output = "duplicate_full.txt";
my $ref = "${Bin}/${outputDirName}/duplicate_full_ref.txt";
$cmd = "diff --brief $output $ref";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
$output = "duplicate.log";
$ref = "${Bin}/${outputDirName}/duplicate_log_ref.txt";
$cmd = "diff --brief $output $ref";
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
