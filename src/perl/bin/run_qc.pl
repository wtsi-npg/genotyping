#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# extract information from PLINK data; generate QC plots and reports
# generic script intended to work with any caller producing PLINK output

# 1. Assemble information in text format; write to given 'qc directory'
# 2. Generate plots

use strict;
use warnings;
use Getopt::Long;
use Cwd;
use FindBin qw($Bin);

my ($help, $outDir, $plinkPrefix);

GetOptions("help"         => \$help,
	   "output_dir=s" => \$outDir,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--output-dir        Directory for QC output
--help              Print this help text and exit
";
    exit(0);
}

$outDir ||= "./qc";
if (not -e $outDir) { mkdir($outDir); }

$plinkPrefix = $ARGV[0];
unless ($plinkPrefix) { die "ERROR: Must supply a PLINK filename prefix!"; }

run($plinkPrefix, $outDir);

sub writeInputFiles {
    # read PLINK output and write text files for input to QC.
    my ($plinkPrefix, $outDir, $verbose) = @_;
    my $crStatsExecutable = "/nfs/users/nfs_i/ib5/mygit/github/Gftools/snp_af_sample_cr_bed"; # TODO current path is a temporary measure for testing; needs to be made portable for production
    my $startDir = getcwd;
    chdir($outDir);
    my $cmd;
    # check identity; writes to current working directory by default
    #system("perl $Bin/check_identity_bed.pl $plinkPrefix"); 
    if ($verbose) { print "Identity check done.\n"; }
    # find sample & snp call rates etc; need this *before* duplicate check
    #system("$crStatsExecutable $plinkPrefix"); 
    if ($verbose) { print "SNP and sample call stats done.\n"; }
    # check duplicates
    #system("perl $Bin/check_duplicates_bed.pl $plinkPrefix"); 
    if ($verbose) { print "Duplicate check done.\n"; }
    # write gender file
    system("perl $Bin/write_gender_files.pl --qc-output=sample_xhet_gender.txt --plots-dir=. $plinkPrefix");
    if ($verbose) { print "Gender check done.\n"; }
    chdir($startDir);
}

sub run {
    # main method to run script
    my ($plinkPrefix, $outDir, $verbose) = @_;
    $verbose ||= 1;
    writeInputFiles($plinkPrefix, $outDir, $verbose);
}
