# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# test identity check against Sequenom results

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use FindBin qw/$Bin/;
use JSON;
use Test::More tests => 16;
use WTSI::NPG::Genotyping::QC::Identity qw/run_identity_check/;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/readFileToString/;
use WTSI::NPG::Genotyping::QC::SnpID qw/illuminaToSequenomSNP sequenomToIlluminaSNP/;

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

my $dataDir = "/nfs/gapi/data/genotype/pipeline_test/identity_check";

my @plink = qw/identity_test identity_test_not_exome/;
my $jsonName = 'identity_check.json';
my @outputs = ($jsonName, 'identity_check_results.txt', 'identity_check_failed_pairs.txt');
my ($cmd, $workDir, $workRef, $dataRef, $failDataRef, $outJsonPath);
my $refJsonPath = $dataDir.'/'.$jsonName;
$dataRef = decode_json(readFileToString($dataDir.'/'.$jsonName));
$failDataRef = decode_json(readFileToString($dataDir.'/identity_check_fail.json'));
my $minSNPs = 8;
my $largeNumberOfSNPs = 1000; # use to make methods fail
my $minIdent = 0.90;
my $swap = 0.95;
my $iniPath = $ENV{HOME} . "/.npg/genotyping.ini";

foreach my $plink (@plink) {
    my $input = $dataDir."/".$plink;
    $workDir = tempdir("identity_test_XXXXXX", CLEANUP => 1);
    $outJsonPath = $workDir.'/identity_check.json';
    run_identity_check($input, $workDir, $minSNPs, $minIdent, $swap, $iniPath);
    foreach my $output (@outputs) {
        my $ref = $dataDir."/".$output;
        my $check = "$workDir/$output";
	if ($output eq $jsonName) {
	    $workRef = decode_json(readFileToString($workDir.'/'.$jsonName));
	    is_deeply($workRef, $dataRef, "Compare output JSON data structure against reference, input $plink");
	} else {
	    my $status = system("diff $ref $check >& /dev/null");
	    is($status, 0, "Diff result: Input $plink, output $output");
	}
    }
    # create new workdir and test command-line script
    $workDir = tempdir("identity_test_XXXXXX", CLEANUP => 1);
    $outJsonPath = $workDir.'/identity_check.json';
    my $cmd = "$bin/check_identity_bed.pl --config $config " .
      "--outdir $workDir --plink $input";
    is(system($cmd), 0, "check_identity_bed.pl exit status, input $plink");
    $workRef = decode_json(readFileToString($outJsonPath));
    is_deeply($workRef, $dataRef, "Compare output JSON data structure from command-line script against reference, input $plink");
    # create (another) new workdir, test behaviour when too few SNPs available
    $workDir = tempdir("identity_test_XXXXXX", CLEANUP => 1);
    run_identity_check($input, $workDir, $largeNumberOfSNPs, $minIdent, $swap, $iniPath, 0);
    $workRef = decode_json(readFileToString($workDir.'/'.$jsonName));
    is_deeply($workRef, $failDataRef, "Compare output JSON data structure after insufficient SNPs failure, input $plink");
}

