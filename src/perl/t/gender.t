# Tests creation of input files for genotyping QC
# Start with running individual scripts and testing exit status
# Later try and validate inputs

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Carp;
use Cwd;
use FindBin qw($Bin);
use File::Temp qw/tempdir/;
use Log::Log4perl;
use JSON;
use Test::More tests => 26;
use WTSI::Genotyping::Database::Pipeline;
use WTSI::Genotyping::QC::GenderCheck;
use WTSI::Genotyping::QC::QCPlotTests qw/createTestDatabase/;

Log::Log4perl::init('etc/log4perl_tests.conf');

my $start = time();
my $dbWorkDir = "$Bin/.."; # must change dir to genoytping/src/perl for default pipeline database config
chdir($dbWorkDir);
my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $plink = "$Bin/qc_test_data/alpha";
my $outDir = "$Bin/gender/";
my $inputDir = $outDir;
my $title = "Alpha";
my $script = "$bin/check_xhet_gender.pl";
my $refFile = "$inputDir/benchmark_gender.json";
my $runName = "pipeline_run";
my $largeInputPath = "$inputDir/sample_xhet_gender_large.txt";
my $largeInputRef = "$inputDir/benchmark_gender_large.json";

# read benchmark genders for comparison
system('rm -f $outDir/*.png $outDir/*.log $outDir/sample_*_.txt'); # remove output from previous tests, if any

my %refGenders = readBenchmark($refFile);
my @names = keys(%refGenders);

foreach my $format qw(plink json text) {
    foreach my $jsonOut ((0,1)) {
        my ($input, $outPath, $outType);
        my $dbfile = createTestDatabase(\@names);
        print "\tCreated temporary database in $dbfile\n";
        if ($format eq 'json') { $input = "$inputDir/input_xhet.json"; } 
        elsif ($format eq 'text') { $input = "$inputDir/input_xhet.txt"; } 
        elsif ($format eq 'plink') { $input = $plink; } 
        my $cmd = "perl $script --run=$runName --input=$input ".
            "--input-format=$format --output-dir=$outDir --dbfile=$dbfile";
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
        is($status, 0, "check_xhet_gender.pl exit status, ".
           "input $format, output type $outType");
        is(diffGenders(\%refGenders, $outPath), 0, 
           "Verify $outType output vs. benchmark");
        my %dbGenders = readDatabaseGenders($dbfile);
        my @names = keys(%dbGenders);
        is(keys(%dbGenders), 995, "Read correct number of inferred genders "
           ."from database");
        is(diffGenders(\%dbGenders, $outPath), 0, 
           "Verify $outType output vs. database");
        system("rm -f $dbfile");
    }
}

# run test on larger input set, with smoothing
%refGenders = readBenchmark($largeInputRef);
@names = keys(%refGenders);
my $input = $inputDir."/sample_xhet_gender_large.txt";
my $tempDir = tempdir( CLEANUP => 1 );
my $cmd = "perl $script --input=$input --output-dir=$tempDir";
my $status = system($cmd);
is($status, 0, "check_xhet_gender.pl exit status, large input dataset");
my $outPath = $tempDir."/sample_xhet_gender.txt";
is(diffGenders(\%refGenders, $outPath), 0, 
   "Verify large input against benchmark");

my $duration = time() - $start;
print "Gender check test finished.  Duration: $duration s\n";

sub diffGenders {
    # compare gender results to benchmark
    my ($benchmarkRef, $inPath) = @_;
    my %benchmark = %$benchmarkRef;
    my $diff = 0;
    my %genders = readGenderOutput($inPath);
    # check gender codes
    foreach my $sample (keys(%genders)) {
        if ($benchmark{$sample} != $genders{$sample}) { 
            print STDERR "\t### $sample $benchmark{$sample} $genders{$sample}\n";
            $diff = 1; 
            #last; 
        }
    }
    # if codes OK, check that sample sets match
    unless ($diff) {
        foreach my $sample (keys(%benchmark)) {
            if (!defined($genders{$sample})) { 
                $diff = 1; 
                print STDERR "\t### \"$sample\" gender not defined.\n";
               #last; 
            }
        }
    }
    return $diff;
}

sub readBenchmark {
    my $inPath = shift;
    open my $in, "< $inPath";
    my @lines = ();
    while (<$in>) {
        chomp;
        push(@lines, $_);
    }
    close $in;
    my $ref = decode_json(join('', @lines));
    my %refGenders = %$ref;
    return %refGenders;
}

sub readGenderOutput {
    # read gender codes from .txt or .json output of check_xhet_gender.pl
    my $inPath = shift;
    open my $in, "<", $inPath;
    my %genders;
    if ($inPath =~ /\.txt$/) {
	my $first = 1;
	while (<$in>) {
	    if ($first) { $first = 0; next; } # skip headers
	    my @words = split;
	    my ($sample, $gender) = ($words[0], $words[2]); # fields are: name, xhet, inferred, supplied
	    $genders{$sample} = $gender;
	}
    } elsif ($inPath =~ /\.json$/) {
	my @lines = ();
	while (<$in>) {
	    chomp;
	    push(@lines, $_);
	}
	my $ref = decode_json(join('', @lines));
	my @records = @$ref;
	foreach my $recRef (@records) {
	    my %record = %$recRef;
	    $genders{$record{'sample'}} = $record{'inferred'};
	}
    } else {
	croak "Illegal filename extension: $inPath";
    }
    close $in;
    return %genders;
}

