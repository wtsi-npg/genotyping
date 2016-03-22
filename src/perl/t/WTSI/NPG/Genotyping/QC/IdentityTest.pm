use utf8;

package WTSI::NPG::Genotyping::QC::IdentityTest;

use strict;
use warnings;
use File::Slurp qw(read_file);
use File::Temp qw(tempdir);
use JSON;
use Log::Log4perl;
use Log::Log4perl::Level;

use base qw(WTSI::NPG::Test);
use Test::More tests => 28;
use Test::Exception;

use WTSI::NPG::Genotyping::QC::Identity;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/defaultJsonConfig/;
use WTSI::NPG::Genotyping::QC::SnpID qw/convertFromIlluminaExomeSNP convertToIlluminaExomeSNP/;

Log::Log4perl::init('./etc/log4perl_tests.conf');
my $log = Log::Log4perl->get_logger();
my $workdir;
my $jsonRef;
my $jsonOutPath;
my $jsonName = 'identity_check.json';
my $textName = 'identity_check_results.txt';
my $gtName = 'identity_check_gt.txt';
my $failPairsName = 'identity_check_failed_pairs.txt';
my $pipelineTestDir = '/nfs/gapi/data/genotype/pipeline_test';
my $dataDir = $pipelineTestDir.'/identity_check';
my $dbPath = $dataDir.'/id_test_genotyping.db';
my $manifest = $pipelineTestDir.'/manifests/Human670-QuadCustom_v1_A.bpm.csv';
my $qcPlex = "$dataDir/W30467_snp_set_info_1000Genomes.tsv";
my $minSNPs = 8;
my $manySNPs = 1000; # use to make methods fail
my $minIdent = 0.90;
my $swap = 0.95;
my $iniPath = $ENV{HOME} . "/.npg/genotyping.ini";

sub setup : Test(setup) {
    $workdir = tempdir("identity_test_XXXXXX", CLEANUP => 1);
    $jsonOutPath = $workdir.'/'.$jsonName;
    $jsonRef = decode_json(read_file($dataDir.'/'.$jsonName));
}

sub teardown : Test(teardown) {
    # placeholder, does nothing for now
}

sub run_identity {
    # run the 'main' identity check method
    my @inputs = @{ shift() };
    ok(run_identity_check(@inputs), "Identity check status, input $inputs[0]");
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC::Identity');
}

sub test_alternate_snp_names : Test(5) {
    WTSI::NPG::Genotyping::QC::Identity->new(
        db_path => $dbPath,
        ini_path => $iniPath,
        output_dir => $workdir,
        plex_manifest => $qcPlex,
        plink_path => $dataDir.'/identity_test_not_exome',
    )->run_identity_check();
    validate_outputs();
}

sub test_command_line : Test(6) {
    my $plink = $dataDir."/identity_test";
    my $config = defaultJsonConfig();
    my $cmd = "check_identity_bed.pl --config $config --outdir $workdir ".
	" --plink $plink --db $dbPath";
    is(system($cmd), 0, "check_identity_bed.pl exit status, input $plink");
    validate_outputs();
}

sub test_manifest_intersection : Test(3) {
    my $expected = 25;
    my $snps = $workdir.'/shared_snps.txt';
    my $cmd = "manifest_plex_intersection.pl --manifest $manifest --plex $qcPlex --out $snps";
    is(system($cmd), 0, 'manifest_plex_intersection.pl exit status');
    ok(-e $snps, 'SNP output text file exists');
    my $total = 0;
    open my $in, "<", $snps || $log->logcroak("Cannot open input '$snps'");
    while (<$in>) { $total++; }
    close $in || $log->logcroak("Cannot close input '$snps'");
    is($total, $expected, "$expected SNPs read from text file");
}

sub test_insufficient_snps : Test(2) {
    WTSI::NPG::Genotyping::QC::Identity->new(
        db_path => $dbPath,
        ini_path => $iniPath,
        min_shared_snps => $manySNPs,
        output_dir => $workdir,
        plex_manifest => $qcPlex,
        plink_path => $dataDir.'/identity_test'
    )->run_identity_check();
    ok(-e $jsonOutPath, "JSON output exists for insufficient SNPs");
    my $failJson = $dataDir.'/identity_check_fail.json';
    my $failDataRef = decode_json(read_file($failJson));
    my $jsonOut = decode_json(read_file($jsonOutPath));
    is_deeply($jsonOut, $failDataRef, "JSON output is equivalent to reference");
}

sub test_name_conversion : Test(4) {
    my $id = 'exm-rs1234';
    is(convertFromIlluminaExomeSNP($id), 'rs1234',
       'Illumina to Sequenom action');
    is(convertToIlluminaExomeSNP($id), 'exm-rs1234',
       'Sequenom to Illumina no action');
    $id = 'rs5678';
    is(convertFromIlluminaExomeSNP($id), 'rs5678',
       'Illumina to Sequenom no action');
    is(convertToIlluminaExomeSNP($id), 'exm-rs5678',
       'Sequenom to Illumina action');
}

sub test_standard : Test(7) {
    my $checker = WTSI::NPG::Genotyping::QC::Identity->new(
        db_path => $dbPath,
        ini_path => $iniPath,
        output_dir => $workdir,
        plex_manifest => $qcPlex,
        plink_path => $dataDir.'/identity_test',
    );
    ok($checker, "Identity check Moose object created");
    ok($checker->run_identity_check(), "Identity check completed");
    validate_outputs();
}

sub validate_outputs {
    # check for output files and validate contents of JSON
    # expects output files for the 'standard' test dataset and parameters
    ok(-e $jsonOutPath, "JSON output exists");
    my $jsonOut = decode_json(read_file($jsonOutPath));
    is_deeply($jsonOut, $jsonRef, "JSON output is equivalent to reference");
    ok(-e $workdir.'/'.$textName, "Text summary exists");
    ok(-e $workdir.'/'.$failPairsName, "Failed pairs comparison exists");
    ok(-e $workdir.'/'.$gtName, "Detailed genotype file exists");
}

return 1;
