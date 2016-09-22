#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Cwd;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::Identity;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(readThresholds);

our $VERSION = '';
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

my ($outDir, $dbPath, $configPath, $iniPath, $minSNPs, $minIdent, $swap,
    $plink, $help);

# TODO introduce 'quiet' mode to suppress warnings

GetOptions("outdir=s"     => \$outDir,
	   "db=s"         => \$dbPath,
           "config=s"     => \$configPath,
           "ini=s"        => \$iniPath,
           "min_snps=i"   => \$minSNPs,
           "min_ident=f"  => \$minIdent,
	   "swap=f"       => \$swap,
	   "plink=s"      => \$plink,
           "h|help"       => \$help);

my $swapDefault = 0.95;

if ($help) {
    print STDERR "Usage: $0 [ output file options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--config=PATH       Config path in .json format with QC thresholds. 
                    At least one of config or min_ident must be given.
--ini=PATH          Path to .ini file with additional configuration. 
                    Defaults to: $DEFAULT_INI
--min_snps=NUMBER   Minimum number of SNPs for comparison
--min_ident=NUMBER  Minimum threshold of SNP matches for identity; if given, overrides value in config file; 0 <= NUMBER <= 1
--swap=NUMBER       Minimum threshold of SNP matches to flag a failed sample
                    pair as a potential swap; 0 <= NUMBER <= 1. Optional, 
                    defaults to $swapDefault.
--outdir=PATH       Directory for output files. Optional, defaults to current 
                    working directory.
--plink=PATH        Prefix for a Plink binary dataset, ie. path without .bed,
                    .bim, .fam extension. Required.
--db=PATH           Path to an SQLite pipeline database containing the QC plex calls. Required.
--help              Print this help text and exit
";
    exit(0);
}

$plink or croak("Must supply a Plink binary input prefix");
foreach my $part (map { $plink . $_ } qw(.bed .bim .fam)) {
  -e $part or croak("Prefix '$plink' is not a valid Plink binary dataset; ",
                    "'", $part, "' is missing");
}

if ($outDir) {
  -e $outDir or croak("Output '", $outDir, "' does not exist");
  -d $outDir or croak("Output '", $outDir, "' is not a directory");
}

$dbPath or croak("Must supply an SQLite pipeline database path");
-e $dbPath or croak("Database path '", $dbPath, "' does not exist");

$outDir ||= getcwd();
$minSNPs ||= 8;
if (!$minIdent) {
    if ($configPath) {
        my %thresholds = readThresholds($configPath);
        $minIdent = $thresholds{'identity'};
    } else {
        croak("Must supply a value for either --min_ident or --config");
    }
}
if ($minIdent < 0 || $minIdent > 1) {
    croak("Minimum identity value must be a number between 0 and 1");
}
if ($swap && ($swap < 0 || $swap > 1)) {
    croak("Swap threshold must be a number between 0 and 1");
}
$swap ||= $swapDefault;

$iniPath ||= $DEFAULT_INI;

WTSI::NPG::Genotyping::QC::Identity->new(
    db_path => $dbPath,
    ini_path => $iniPath,
    min_shared_snps => $minSNPs,
    output_dir => $outDir,
    pass_threshold => $minIdent,
    plink_path => $plink,
    swap_threshold => $swap
)->run_identity_check();


__END__

=head1 NAME

check_identity_bed

=head1 DESCRIPTION

Compare genotype calls to check identity with a QC plex

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2013, 2014, 2015, 2016 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
