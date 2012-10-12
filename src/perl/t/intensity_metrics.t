# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Test::More tests => 4;
use JSON;
use WTSI::Genotyping::QC::QCPlotShared qw/defaultJsonConfig/;
use WTSI::Genotyping::QC::SimFiles qw/headerParams readSampleNames
 writeIntensityMetrics/;

my $testDir = "/nfs/users/nfs_i/ib5/mygit/github/genotyping/src/perl/t/";
my $simPath = $testDir."qc_test_data/alpha.sim";

my $outPathMag = "/tmp/mag.txt";
my $outPathXY = "/tmp/xy.txt";

open my $in, "<", $simPath || croak "Cannot open $simPath";
open my $outMag, ">", $outPathMag || croak "Cannot open outPathMag";
open my $outXY, ">", $outPathXY || croak "Cannot open outPathXY";

my %params = headerParams($in);
is(keys(%params), 9, "Header params of correct length");
my @names = readSampleNames($in, \%params);
is(@names, 995, "Correct number of sample names");

ok(writeIntensityMetrics($in, $outMag, $outXY), "Write intensity metrics");

foreach my $fh ($in, $outMag, $outXY) {
    close $fh || croak "Cannot close filehandle";
}

# again, with small blocks of probes
$outPathMag = "/tmp/mag2.txt";
$outPathXY = "/tmp/xy2.txt";

open $in, "<", $simPath || croak "Cannot open $simPath";
open $outMag, ">", $outPathMag || croak "Cannot open outPathMag";
open $outXY, ">", $outPathXY || croak "Cannot open outPathXY";

ok(writeIntensityMetrics($in, $outMag, $outXY, 10), 
   "Write intensity metrics with small block size");


#open $in, "<", $simPath || croak "Cannot open $simPath";
#open $outMag, ">", $outPathMag || croak "Cannot open outPathMag";
#open $outXY, ">", $outPathXY || croak "Cannot open outPathXY";

#ok(writeIntensityMetrics($in, $outMag, $outXY), "write intensity metrics");

foreach my $fh ($in, $outMag, $outXY) {
    close $fh || croak "Cannot close filehandle";
}
