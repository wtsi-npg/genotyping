#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# Read PLINK genotyping data for X chromosome; write text files containing gender information
# Outputs:  ${SOMETHING}_illuminus_gender.txt (for Illuminus caller)
#           sample_xhet_gender.txt (general purpose)

use strict;
use warnings;
use File::Temp qw(tempdir);
use Getopt::Long;
use plink_binary; # in /software/varinf/gftools/lib ; front-end for C library

my $help;
my ($noModel, $plinkPrefix, $outDir);

GetOptions("h|help"        => \$help,
	   "output_dir=s"  => \$outDir,
	   "no-model"      => \$noModel,
    );

if ($help) {
    print STDERR "Usage: $0 [ output file options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--help              Print this help text and exit
--output-dir        Directory for gender output files (including model parameters)
--no-model          Do not use mixture model to calculate xhet gender thresholds (mixture model is used by default)
Unspecified options will receive default values, with output written to current directory.
";
    exit(0);
}

$plinkPrefix = $ARGV[0];
unless ($plinkPrefix) { die "Must supply a PLINK filename prefix!"; }
unless ($outDir) { die "Must supply an output directory!"; }
my $tempdir = $ARGV[1]; ### for testing; normally leave this empty to create a temporary directory
run($plinkPrefix, $outDir, $tempdir);

sub extractChromData {
    # write plink binary data to given directory, for given chromosome only (defaults to X)
    # return plink_binary object for reading data
    my ($plinkPrefix, $outDir, $chrom, $outPrefix) = @_;
    $chrom ||= "X";
    $outPrefix ||= "plink_temp_chrom$chrom";
    my $outArg = $outDir."/$outPrefix";
    my $cmd = "/software/bin/plink --bfile $plinkPrefix --chr $chrom --out $outArg --make-bed";
    system($cmd);
    my $pb = new plink_binary::plink_binary($outArg);
    return $pb;
}

sub findHetRates {
    # find het rates by sample for given plink binary and sample names
    my $pb = shift;
    my @sampleNames = @{shift()};
    my $minCR = shift; # minimum SNP call rate
    $minCR ||= 0.95;
    my %allHets = ();
    my %allCalls = ();
    foreach my $name (@sampleNames) { $allHets{$name} = 0; }
    my $snps = 0;
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    my $snpFail = 0;
    while ($pb->next_snp($snp, $genotypes)) {
	# get calls for each SNP; if call rate OK, update het count for each sample
	# het rate defined as: successful calls / het calls for each sample, on snps satisfying min call rate
	my $snp_id = $snp->{"name"};
	my $samples = 0;
	my $noCalls = 0;
	my %hets = ();
	my %calls = ();
	for my $i (0..$genotypes->size() - 1) {
	    my $call = $genotypes->get($i);
	    $samples++;
	    if ($call =~ /[N]{2}/) { 
		$noCalls++;
	    } else {
		$calls{$sampleNames[$i]} = 1;
		if (substr($call, 0, 1) ne substr($call, 1, 1)) { $hets{$sampleNames[$i]} = 1; }
	    }
	}
	if (1-($noCalls/$samples) >= $minCR) {
	    foreach my $sample (keys(%calls)) { $allCalls{$sample}++; }
	    foreach my $sample (keys(%hets)) { $allHets{$sample}++; }
	    $snps++;
	    my $cr = 1 - $noCalls/$samples;
	    if ($snps % 1000 == 0) { print $snps." ".$cr."\n"; }
	} else {
	    $snpFail++;
	}
    }
    if ($snps==0) { print STDERR "WARNING: No valid SNPs found for X evaluation!"; die; } 
    else { print "$snps X SNPs passed, $snpFail failed\n"; }
    my %hetRates = ();
    for my $i (0..$genotypes->size() - 1) {
	my $name = $sampleNames[$i];
	my $hetRate;
	if ($allCalls{$name} > 0) { $hetRate = $allHets{$name} / $allCalls{$name}; }
	else { $hetRate = 0; }
	$hetRates{$name} = $hetRate;
	if ($i % 100 == 0) {print $i." ".$sampleNames[$i]." ".$hetRate."\n"; }
    }
    print $snps." X SNPs found.\n";
    return %hetRates;
}

sub getSampleNames {
    # get sample names from given plink binary object
    my $pb = shift;
    my @sampleNames;
    for my $i (0..$pb->{"individuals"}->size() - 1) {
	my $name = $pb->{"individuals"}->get($i)->{"name"};
	if ($i % 100 == 0) {print $i." ".$name."\n"; }
	push(@sampleNames, $name);
    }
    return @sampleNames;
}

sub writeHetRates {
    # write het rates for each sample to a tab-delimited text file; use as input to R scripts
    # could just iterate over keys of %hetRates, but separate @sampleNames list preserves name order
    my %hetRates = %{shift()};
    my @sampleNames = @{shift()};
    my $outPath = shift;
    open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
    print OUT "sample\txhet\n"; # column headers; must be as expected by check_xhet_gender.R !
    foreach my $name (@sampleNames) {
	my $out = sprintf("%s\t%.6f\n", ($name, $hetRates{$name}));
	#print $out;
	print OUT $out;
    }
    close OUT;
}

sub run {
    # 'main' method to run script
    my $plinkPrefix = shift;
    my $outDir = shift;
    my $tempdir = shift;
    #$tempdir ||= tempdir(CLEANUP => 1); # CLEANUP deletes this tempdir on script exit
    $tempdir ||= tempdir();
    print "### $tempdir\n";
    my $pb = extractChromData($plinkPrefix, $tempdir);
    my @sampleNames = getSampleNames($pb);
    my %hetRates = findHetRates($pb, \@sampleNames);
    my $tempPath = $tempdir."/sample_xhet.txt";
    writeHetRates(\%hetRates, \@sampleNames, $tempPath);
    my $cmd = "perl ./check_xhet_gender.pl --input=$tempPath --output_dir=$outDir";
    print $cmd."\n";
    system($cmd);
}
