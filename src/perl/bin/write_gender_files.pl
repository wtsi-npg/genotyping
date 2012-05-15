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
my ($noModel, $plinkPrefix);

GetOptions("h|help"      => \$help,
	   "no-model"    => \$noModel,
    );

if ($help) {
    print STDERR "Usage: $0 [ output file options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--help              Print this help text and exit
--no-model          Do not use mixture model to calculate xhet gender thresholds (mixture model is used by default)
Unspecified options will receive default values, with output written to current directory.
";
    exit(0);
}

$plinkPrefix = $ARGV[0];
unless ($plinkPrefix) { die "Must supply a PLINK filename prefix!"; }

my $tempdir = tempdir(CLEANUP => 1); # CLEANUP deletes this tempdir on script exit
#my $tempdir = tempdir(); # CLEANUP deletes this tempdir on script exit
my $tempPrefix = $tempdir.'/plink_temp_Xchrom';
print $tempPrefix."\n";

# use plink application directly to extract X chromsome calls and write to temporary directory
my $cmd = "/software/bin/plink --bfile $plinkPrefix --chr X --out $tempPrefix --make-bed";
print $cmd."\n";
system($cmd);

# now use Perl front-end to read X calls from temporary directory
# just count SNPs to test; later will write to temporary text file
my $pb = new plink_binary::plink_binary($plinkPrefix); # $pb = object to parse given PLINK files
my $snp = new plink_binary::snp;
my $genotypes = new plink_binary::vectorstr;
my $xTotal = 0;
my $noCalls = 0;
my $hetCalls = 0;
my $homCalls = 0;
while ($pb->next_snp($snp, $genotypes)) {
    # this iterates over SNPs; but also need xhet for each *sample*
    # cf. readPlinkCalls in check_identity_bed
    my $snp_id = $snp->{"name"};
    my $snp_chrom = $snp->{"chromosome"};
    my $allele_a =  $snp->{"allele_a"};
    my $allele_b =  $snp->{"allele_b"};
    if ($allele_a !~ /[ACGT]/ || $allele_b !~ /[ACGT]/) { $noCalls++; }
    elsif ($allele_a ne $allele_b) { $hetCalls++; }
    else { $homCalls++; }
    #print $snp_id."\t".$snp_chrom."\n";
    if ($snp_chrom == 23) { 
	$xTotal++; 
	if ($xTotal % 1000 == 0) { print $allele_a." ".$allele_b." ".$xTotal."\n"; }
    } else {
	print "WARNING: Incorrect chromosome: $snp_id $snp_chrom \n";
    }
}
print $xTotal." X SNPs found.\n";
print $noCalls." no calls.\n";
print $hetCalls." het calls.\n";
print $homCalls." hom calls.\n";
