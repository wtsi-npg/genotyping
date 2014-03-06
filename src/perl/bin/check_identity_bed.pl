#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Cwd;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::Identity qw(run_identity_check);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(readThresholds);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

# Check identity of genotyped data against sequenom
# Input: files of genotypes in tab-delimited format, one row per SNP

# Author:  Iain Bancarz, ib5@sanger.ac.uk (refactored edition Feb 2012, original author unknown)

# Old version used heterozygosity mismatch rates for comparison
# Modify to use genotype mismatch rates
# Do not count "flips" and/or "swaps" as mismatches
# Flip:  Reverse complement, eg. GA and TC
# Swap:  Transpose major and minor alleles, eg. GA and AG
# can have both flip and swap, eg. GA ~ CT

# IMPORTANT:  Plink and Sequenom name formats may differ:
# - Plink *sample* names may be of the form PLATE_WELL_ID
#   where ID is the Sequenom identifier
# - Plink *snp* names may be of the form exm-FOO
#   where FOO is the Sequenom SNP name
# - Either of the above differences *may* occur, but is not guaranteed!

my $help;
my ($outDir, $outputGT, $outputResults,  $outputFail, $outputFailedPairs, 
    $outputFailedPairsMatch, $configPath, $iniPath,
    $minCheckedSNPs, $minIdent, $output, $log);

GetOptions("outdir=s"     => \$outDir,
           "config=s"     => \$configPath,
           "ini=s"        => \$iniPath,
           "min_snps=i"   => \$minCheckedSNPs,
           "min_ident=f"  => \$minIdent,
           "h|help"       => \$help);

if ($help) {
    print STDERR "Usage: $0 [ output file options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--outdir=PATH       Output directory for results files. Optional, defaults 
                    to current working directory.
--config=PATH       Config path in .json format with QC thresholds. 
                    At least one of config or min_ident must be given.
--ini=PATH          Path to .ini file with additional configuration. 
                    Defaults to: $DEFAULT_INI
--min_snps=NUMBER   Minimum number of SNPs for comparison
--min_ident=NUMBER  Minimum threshold of SNP matches for identity; if given, overrides value in config file; 0 <= NUMBER <= 1
--help              Print this help text and exit
Unspecified options will receive default values, with output written to current directory.
";
    exit(0);
}

my $plinkPrefix = $ARGV[0];
if (!$plinkPrefix) { die "Must supply a Plink genotype file prefix: $!"; }

# parameter default values
if ($outDir && !(-e $outDir && -d $outDir)) {
    croak "Output path $outDir does not exist or is not a directory!";
}
$outDir ||= getcwd();
$minCheckedSNPs ||= 20;
if (!$minIdent) {
    if ($configPath) {
        my %thresholds = readThresholds($configPath);
        $minIdent = $thresholds{'identity'};
    } else {
        croak("Must supply a value for either --min_ident or --config");
    }
}
$iniPath ||= $DEFAULT_INI;

run_identity_check($plinkPrefix, $outDir, $minCheckedSNPs, $minIdent, $iniPath);

