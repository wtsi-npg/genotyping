# Tests creation of input files for genotyping QC
# Start with running individual scripts and testing exit status
# Later try and validate inputs

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Cwd;
use FindBin qw($Bin);
use JSON;
use Test::More tests => 12;

my $start = time();
my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $plink = "$Bin/qc_test_data/alpha";
my $outDir = "$Bin/gender/";
my $title = "Alpha";
my $script = "$bin/check_xhet_gender.pl";
my $refFile = "benchmark_gender.json";

# read benchmark genders for comparison
chdir($outDir);
system('rm -f *.png *.log sample_*_.txt'); # remove output from previous tests, if any
open my $in, "< $refFile";
my @lines = ();
while (<$in>) {
    chomp;
    push(@lines, $_);
}
close $in;
my $ref = decode_json(join('', @lines));
my %refGenders = %$ref;

foreach my $format qw(json text plink) {
    my $input;
    if ($format eq 'json') { $input = 'input_xhet.json'; } 
    elsif ($format eq 'text') { $input = 'input_xhet.txt'; } 
    elsif ($format eq 'plink') { $input = $plink; } 
    my $cmd = "perl $script --input=$input --input-format=$format --output-dir=.";
    my $status = system($cmd);
    is($status, 0, "check_xhet_gender.pl exit status, input $format, output text");
    is(diffGenders(\%refGenders, 'sample_xhet_gender.txt'), 0, "Verify .txt output against benchmark");
    $cmd .= " --json";
    $status = system($cmd);
    is($status, 0, "check_xhet_gender.pl exit status, input $format, output json");
    is(diffGenders(\%refGenders, 'sample_xhet_gender.json'), 0, "Verify .json output against benchmark");
}

my $duration = time() - $start;
print "Gender check test finished.  Duration: $duration s\n";

sub diffGenders {
    # compare gender results to benchmark
    my ($benchmarkRef, $inPath) = @_;
    my %benchmark = %$benchmarkRef;
    my $diff = 0;
    open($in, "< $inPath");
    if ($inPath =~ /\.txt$/) {
	my $first = 1;
	while (<$in>) {
	    if ($first) { $first = 0; next; } # skip headers
	    my @words = split;
	    my ($sample, $gender) = ($words[0], $words[2]); # fields are: name, xhet, inferred, supplied
	    if ($benchmark{$sample} != $gender) { $diff = 1; last; }
	}
    } else {
	my @lines = ();
	while (<$in>) {
	    chomp;
	    push(@lines, $_);
	}
	my $ref = decode_json(join('', @lines));
	my @records = @$ref;
	foreach my $recRef (@records) {
	    my %record = %$recRef;
	    if ($benchmark{$record{'sample'}} != $record{'inferred'}) { $diff = 1; last; }
	}
    }
    close $in;
    return $diff;
}
