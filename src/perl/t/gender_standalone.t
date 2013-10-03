# Test of 'gendermix' standalone gender check

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# January 2013

use strict;
use warnings;
use Carp;
use Cwd qw/abs_path/;
use FindBin qw($Bin);
use File::Temp qw/tempdir/;
use Log::Log4perl;
use JSON;
use Test::More tests => 12;
use WTSI::NPG::Genotyping::QC::GenderCheck;

# The directory contains the R scripts
$ENV{PATH} = abs_path('../r/bin') . ':' . $ENV{PATH};

Log::Log4perl::init('etc/log4perl_tests.conf');

my $start = time();
my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $plink = "$Bin/gender/alpha";
my $outDir = "$Bin/gender/";
my $inputDir = $outDir;
my $title = "Alpha";
my $script = "$bin/gendermix_standalone.pl";
my $refFile = "$inputDir/benchmark_gender.json";
my $largeInputPath = "$inputDir/sample_xhet_gender_large.txt";
my $largeInputRef = "$inputDir/benchmark_gender_large.json";

# read benchmark genders for comparison

my %refGenders = readBenchmark($refFile);
my @names = keys(%refGenders);

foreach my $format qw/plink json text/ {
    foreach my $jsonOut ((0,1)) {
        system('rm -f $outDir/*.png $outDir/*.log $outDir/sample_*.txt'); 
        my ($input, $outPath, $outType);
        if ($format eq 'json') { $input = "$inputDir/input_xhet.json"; } 
        elsif ($format eq 'text') { $input = "$inputDir/input_xhet.txt"; } 
        elsif ($format eq 'plink') { $input = $plink; } 
        my $cmd = "perl $script --input=$input ".
            "--input-format=$format --output-dir=$outDir";
         if ($jsonOut) { 
            $cmd .= " --json"; 
            $outPath = "$outDir/sample_xhet_gender.json";
            $outType = 'json';
        } else { 
            $outPath = "$outDir/sample_xhet_gender.txt";
            $outType = 'text';
        }
        my $status = system($cmd);
        ## start tests
        is($status, 0, "gendermix_standalone.pl exit status, ".
           "input $format, output type $outType");
        is(diffGenders(\%refGenders, $outPath), 0, 
           "Verify $outType output vs. benchmark");
    }
}

my $duration = time() - $start;
print "Standalone gender check test finished.  Duration: $duration s\n";

