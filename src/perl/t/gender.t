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
use JSON;
use Test::More tests => 24;
use WTSI::Genotyping::Database::Pipeline;
use WTSI::Genotyping::QC::GenderCheck;

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

# read benchmark genders for comparison
system('rm -f $outDir/*.png $outDir/*.log $outDir/sample_*_.txt'); # remove output from previous tests, if any
open my $in, "< $refFile";
my @lines = ();
while (<$in>) {
    chomp;
    push(@lines, $_);
}
close $in;
my $ref = decode_json(join('', @lines));
my %refGenders = %$ref;
my @names = keys(%refGenders);

foreach my $format qw(plink json text) {
    foreach my $jsonOut ((0,1)) {
	my ($input, $outPath, $outType);
	my $dbfile = createTestDatabase(\@names);
	print "\tCreated temporary database in $dbfile\n";
	if ($format eq 'json') { $input = "$inputDir/input_xhet.json"; } 
	elsif ($format eq 'text') { $input = "$inputDir/input_xhet.txt"; } 
	elsif ($format eq 'plink') { $input = $plink; } 
	my $cmd = "perl $script --input=$input --input-format=$format --output-dir=$outDir --dbfile=$dbfile";
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
	is($status, 0, "check_xhet_gender.pl exit status, input $format, output type $outType");
	is(diffGenders(\%refGenders, $outPath), 0, "Verify $outType output vs. benchmark");
	my %dbGenders = readDatabaseGenders($dbfile);
	ok(%dbGenders, "Read inferred genders from database");
	is(diffGenders(\%dbGenders, $outPath), 0, "Verify $outType output vs. database");
	system("rm -f $dbfile");
    }
}

my $duration = time() - $start;
print "Gender check test finished.  Duration: $duration s\n";

sub createTestDatabase {
    # create temporary test database with given sample names
    # takes $ini_path from GenderCheck.pm
    my @names = @{ shift() };
    my $dbfile = tempdir(CLEANUP => 1).'/pipeline.db'; # remove database file on successful script exit
    #my $ini_path = '/nfs/users/nfs_i/ib5/mygit/github/genotyping/src/perl/etc/';
    my $db = WTSI::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => "$ini_path/pipeline.ini",
	 dbfile => $dbfile);
    my $schema = $db->connect(RaiseError => 1,
			      on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
    $db->populate;
    ## (supplier, snpset, good) table objects are all required 
    my $supplier = $db->datasupplier->find_or_create({name => $ENV{'USER'},
						      namespace => 'wtsi'});
    my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
    my $good = $db->state->find({name => 'Good'});
    ## additional database setup
    my $run = $db->piperun->find_or_create({name => 'paperstreet',
					    start_time => time()});
    my $dataset = $run->add_to_datasets({if_project => "mayhem",
				     datasupplier => $supplier,
				     snpset => $snpset});
    my $supplied = $db->method->find({name => 'Supplied'});
    # fill in with sample names and dummy gender values
    $db->in_transaction(sub {
	foreach my $i (0..@names-1) {
	    my $sample = $dataset->add_to_samples
		({name => $names[$i],
		  state => $good,
		  beadchip => 'ABC123456',
		  include => 1});
	    my $gender = $db->gender->find({name => 'Not Available'});
	    $sample->add_to_genders($gender, {method => $supplied});
	}
			});
    $db->disconnect();
    return $dbfile;
}

sub diffGenders {
    # compare gender results to benchmark
    my ($benchmarkRef, $inPath) = @_;
    my %benchmark = %$benchmarkRef;
    my $diff = 0;
    my %genders = readGenderOutput($inPath);
    # check gender codes
    foreach my $sample (keys(%genders)) {
	#print "### $sample,$benchmark{$sample},$genders{$sample}\n";
	if ($benchmark{$sample} != $genders{$sample}) { $diff = 1; last; }
    }
    # if codes OK, check that sample sets match
    unless ($diff) {
	foreach my $sample (keys(%benchmark)) {
	    unless (defined($genders{$sample})) { $diff = 1; last; }
	}
    }
    return $diff;
}

sub readGenderOutput {
    # read gender codes from .txt or .json output of check_xhet_gender.pl
    my $inPath = shift;
    open($in, "< $inPath");
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

