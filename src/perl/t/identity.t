# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# test identity check against Sequenom results

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Test::More tests => 12;
use WTSI::NPG::Genotyping::QC::SnpID qw/illuminaToSequenomSNP 
  sequenomToIlluminaSNP/;

my $bin = "$Bin/../bin/"; # assume we are running from perl/t

print "\tTranslation between Sequenom and Illumina SNP naming conventions:\n";
my $id = 'exm-rs1234';
is(illuminaToSequenomSNP($id), 'rs1234', 'Illumina to Sequenom action');
is(sequenomToIlluminaSNP($id), 'exm-rs1234', 'Sequenom to Illumina no action');
$id = 'rs5678';
is(illuminaToSequenomSNP($id), 'rs5678', 'Illumina to Sequenom no action');
is(sequenomToIlluminaSNP($id), 'exm-rs5678', 'Sequenom to Illumina action');

print "\tIdentity check against Sequenom for genotyping pipeline QC:\n";
# assume check_identity_bed.pl visible in current path
my $config = $Bin."/../etc/qc_config.json";

# Created a cut-down PLINK dataset (20 SNPs, 5 samples)
# see gapi/genotype_identity_test.git on http://git.internal.sanger.ac.uk
# data contains some "real" samples and calls, so not made public on github

my $dataDir = "/nfs/gapi/data/genotype/qc_test/identity_check/";
my @plink = qw/identity_test identity_test_not_exome/;
my @outputs = qw/identity_check_results.txt identity_check_failed_pairs.txt 
  identity_check_failed_pairs_match.txt/;
my $workdir = "$Bin/identity/";
my $cmd;
chdir($workdir);
foreach my $plink (@plink) {
    my $input = $dataDir."/".$plink;
    $cmd = "$bin/check_identity_bed.pl --config $config $input";
    is(system($cmd), 0, "$bin/check_identity_bed.pl exit status, input $plink");
    foreach my $output (@outputs) {
        my $ref = $dataDir."/".$output;
        my $status = system("diff $ref $output >& /dev/null");
        is($status, 0, "Diff result: Input $plink, output $output");
    }
}

# TODO create a fake .json results path, in order to test identity scatterplot
# put this in main 'alpha' test data set?
