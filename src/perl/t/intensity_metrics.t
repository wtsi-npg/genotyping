# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use File::Temp qw(tempdir);
use Test::More tests => 3;
use JSON;
use WTSI::Genotyping::QC::QCPlotShared qw/defaultJsonConfig/;
use WTSI::Genotyping::QC::SimFiles qw/headerParams readSampleNames
 writeIntensityMetrics/;

my $testDir = "/nfs/users/nfs_i/ib5/mygit/github/genotyping/src/perl/t/";
my $simPath = $testDir."qc_test_data/small_test.sim";

my $temp = tempdir( CLEANUP => 1 );
my $outPathMag = $temp."/mag.txt";
my $outPathXY = $temp."/xy.txt";
my $logPath = $temp."/intensity.log";

open my $in, "<", $simPath || die "Cannot open .sim path $simPath: $!";
my %params = headerParams($in);
is(keys(%params), 9, "Header params of correct length");
my @names = readSampleNames($in, \%params);
is(@names, 100, "Correct number of sample names");
close $in || die "Cannot close .sim path $simPath: $!";

ok(writeIntensityMetrics($simPath, $outPathMag, $outPathXY, $logPath), 
   "Write intensity metrics");
