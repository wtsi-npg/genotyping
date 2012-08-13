#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk (refactored edition, original author unknown)
# Extensively refactored and simplified version (Feb 2012)

# TODO?  Possible extra features; some previously had partial, non-functioning implementation.
# - Filter input data by sample call rate
# - Specify full/summary output filenames (requires correct arguments to pairwise_concordance_bed)
# - Set concordance threshold for duplicate pairs (requires correct arguments to pairwise_concordance_bed)
# - Supply arbitrary lists of samples/SNPs to exclude from QC

use warnings;

use strict;
use Getopt::Long;
use File::Temp;

# Perl wrapper for binary executable pairwise_concordance_bed
# Runs a duplicate check on a set of genotypes in binary ped format.
# Generates a *test set* of (at most) 400 SNPs, filtered by call rate, MAF and separation in genome

# Inputs:
# File containing SNP allele frequencies (defaults to snp_cr_af.txt)
# PLINK genotype output files:  .bim, .bed, .fam
# Input files must be in current working directory

# Outputs:
# duplicate_full.txt, results of pairwise comparison for all SNPs in test set
# duplicate_summary.txt, as above but only for pairs which differ
# duplicate.log, basic log info

my (%defaults, %snps, %pos, %chr);
my ($bfile, $af, $log, $sample_cr, $help, $help_text);
my ($maf_min, $maf_max, $max_snps, $min_snp_cr, $min_dist);
my @use_snps;

# hash of default parameters
%defaults = (af =>  "snp_cr_af.txt",
	     log => "duplicate.log",
	     maf_min => 0.4,
	     maf_max => 0.45,
	     max_snps => 400,
	     min_dist => 1_000_000,
	     min_snp_cr => 0.95,
    );

GetOptions("af=s"        => \$af,       # SNP file with call rate (CR), allele frequency (AF)
           "log=s"       => \$log,
           "min_dist=i"  => \$min_dist, # minimum distance between adjacent SNPs to use in QC test
           "maf_min=s"   => \$maf_min,  # only SNPs with MAF in range
           "maf_max=s"   => \$maf_max,
	   "max=i"       => \$max_snps, # max number of SNPs to use in test
	   "sample_cr=s" => \$sample_cr, # does nothing, but kept in for backwards compatibility of workflows
	   "min_snp_cr=i"=> \$min_snp_cr, # min call rate for snps in test set
	   "help|h"      => \$help,     # print help string and exit
    );
# set unspecified parameters to default values
$af ||= $defaults{'af'};
$log ||= $defaults{'log'};
$maf_min ||= $defaults{'maf_min'};
$maf_max ||= $defaults{'maf_max'};
$max_snps ||= $defaults{'max_snps'};
$min_dist ||= $defaults{'min_dist'};
$min_snp_cr ||= $defaults{'min_snp_cr'};
$bfile = $ARGV[0]; # prefix for PLINK data files

$help_text = "Generate a test set of SNPs; use concordance on test set to identify duplicate samples.
Input files (including PLINK .bed and .bim) must be in current working directory.
Usage:  check_duplicates_bed [OPTIONS] [PLINK prefix]
Options:
--af=NAME   SNP file with call rate (CR), allele frequency (AF); default = $defaults{'af'}
--log=PATH  Path to logfile; default = $defaults{'log'}
--min_dist=DIST  Minimum distance within chromosome for SNPs in test set; default = $defaults{'min_dist'}
--maf_min=MIN  Minimum MAF (minor allele frequency) for SNPs in test set; default = $defaults{'maf_min'}
--maf_max=MAX  Maximum MAF (minor allele frequency) for SNPs in test set; default =  $defaults{'maf_max'}
--max=MAX_SNPS  Maximum number of SNPs in test set; default = $defaults{'max_snps'}
--min_snp_cr=CR  Minimum call rate for SNPs in test set; default = $defaults{'min_snp_cr'}
-h, --help  Print this help message and exit.
";

# basic checks on input
if ($help) {
    print $help_text; exit;
}

# generate hashes of chromosome and position for each SNP in .bim file
open my $bim, "<", $bfile.".bim" or die qq(Unable to open $bfile.bim);
while (<$bim>) {
    my ($chr, $snp, $pos) = (split)[0, 1, 3];
    $chr{$snp} = $chr;
    $pos{$snp} = $pos;
}
close $bim;

# open genotyping SNP results file (defaults to snp_cr_af.txt) and filter on MAF and CR
# populate a hash %snps: Keys=chromosomes, values = lists of (snp_name, snp_position) pairs
open my $snpfile, "<", $af or die $!;
while (<$snpfile>) {
    my ($snp, $cr, $maf) = (split)[0, 1, 5];
    next unless $chr{$snp};
    next unless $cr >= $min_snp_cr;
    next if $maf < $maf_min || $maf > $maf_max;
    push @{$snps{$chr{$snp}}}, [ $snp, $pos{$snp} ];
}
close $snpfile or die $!;

# filter to ensure minimum distance between SNPs; generates @use_snps array
foreach my $chr (keys %snps) {
    @{$snps{$chr}} = sort { $a->[1] <=> $b->[1] } @{$snps{$chr}}; # sort snps in this chromosome by position
    my $last_pos;
    foreach my $snpref (@{$snps{$chr}}) {
        my ($snp, $pos) = @{$snpref};
	if (!$last_pos || $pos > $last_pos + $min_dist) {
	    # keep first SNP position; ignore SNPs within $min_dist of last SNP used
	    push @use_snps, $snp;
            $last_pos = $pos;
	}
    }
}
open my $logfile, ">", $log or die $!;
print $logfile scalar(@use_snps), " available SNPs found for duplicate check\n";
if (@use_snps > $max_snps) {
    @use_snps = @use_snps[0 .. $max_snps - 1]; # truncate @use_snps if too large
}
print $logfile "Using ", scalar(@use_snps), " SNPs\n";
close $logfile;

# write @use_snps to temp file
my $snp_file = new File::Temp; 
$snp_file->autoflush(1);
print $snp_file map { $_ . "\n" } @use_snps;

# execute binary to write pairwise concordance 
my $cmd = "/software/varinf/bin/genotype_qc/pairwise_concordance_bed -n $snp_file $bfile";
system($cmd) && die qq(Error running command "$cmd": $!);

