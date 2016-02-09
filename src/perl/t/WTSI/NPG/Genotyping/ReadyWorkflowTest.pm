use utf8;

package WTSI::NPG::Genotyping::ReadyWorkflowTest;

use strict;
use warnings;
use File::Path qw/make_path/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw/tempdir/;
use JSON;
use Log::Log4perl;
use YAML qw/LoadFile/;

use base qw(WTSI::NPG::Test);
use Test::More tests => 1;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $log = Log::Log4perl->get_logger();

our @fluidigm_csv = qw/fluidigm_001.csv fluidigm_002.csv
                       fluidigm_003.csv fluidigm_004.csv/;
our @sequenom_csv = qw/sequenom_001.csv sequenom_002.csv
                       sequenom_003.csv sequenom_004.csv/;

### variables from ReadyPlexCallsTest.pm

my $irods;
my $irods_tmp_coll;
my $pid = $$;
my $data_path = './t/ready_workflow';
my $tmp;

# fluidigm test data
my $f_reference_name = "Homo_sapiens (1000Genomes)";
my $f_snpset_id = 'qc';
my $f_snpset_filename = 'qc_fluidigm_snp_info_GRCh37.tsv';
my $f_sample_json = catfile($data_path, "fluidigm_samples.json");
my $f_params_name = "params_fluidigm.json";

# sequenom test data
my $s_reference_name = "Homo_sapiens (1000Genomes)";
my $s_snpset_id = 'W30467';
my $s_snpset_filename = 'W30467_snp_set_info_GRCh37.tsv';
my $s_snpset_filename_1 = 'W30467_snp_set_info_GRCh37_1.tsv';
my $s_sample_json = catfile($data_path, "/sequenom_samples.json");
my $s_params_name = "params_sequenom.json";

my @sample_ids = qw(sample_001 sample_002 sample_003 sample_004);
my $chromosome_json_filename = "chromosome_lengths_GRCh37.json";
my $cjson_irods;
my $testnum = 0; # text fixture count


# TODO upload test data to iRODS and ready workflow directory

# TODO ??? make a role for iRODS test fixture creation
# shared between ReadyWorkflowTest, ReadyPlexCallsTest, VCFTest

# setup methods from ReadyPlexCallsTest.pm

sub setup: Test(setup) {

    $tmp = tempdir("ready_workflow_test_XXXXXX", CLEANUP => 0);
    $log->info("Created temporary directory $tmp");
    $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = "ReadyWorkflowTest.$pid.$testnum";
    $testnum++;
    $irods->add_collection($irods_tmp_coll);
    $irods_tmp_coll = $irods->absolute_path($irods_tmp_coll);
    $cjson_irods = $irods_tmp_coll."/".$chromosome_json_filename;

    # set up dummy fasta reference
    $ENV{NPG_REPOSITORY_ROOT} = $tmp;
    my $fastadir = catfile($tmp, 'references', 'Homo_sapiens',
                           'GRCh37_53', 'all', 'fasta');
    make_path($fastadir);
    my $reference_file_path = catfile($fastadir,
                                      'Homo_sapiens.GRCh37.dna.all.fa');
    open my $fh, '>>', $reference_file_path || $log->logcroak(
        "Cannot open reference file path '", $reference_file_path, "'");
    close $fh || $log->logcroak(
        "Cannot close reference file path '", $reference_file_path, "'");
    setup_fluidigm();
    setup_sequenom();
}


sub setup_fluidigm {
    # add some dummy fluidigm CSV files to the temporary collection
    # add sample and snpset names to metadata
    for (my $i=0;$i<@fluidigm_csv;$i++) {
        my $input = $fluidigm_csv[$i];
        my $ipath = $irods_tmp_coll."/".$input;
        $irods->add_object($data_path."/".$input, $ipath);
        $irods->add_object_avu($ipath,'dcterms:identifier', $sample_ids[$i]);
        $irods->add_object_avu($ipath, 'fluidigm_plex', $f_snpset_id);
    }
    my $snpset_path = $irods_tmp_coll."/".$f_snpset_filename;
    $irods->add_object($data_path."/".$f_snpset_filename, $snpset_path);
    $irods->add_object_avu($snpset_path, 'chromosome_json', $cjson_irods);
    $irods->add_object_avu($snpset_path, 'fluidigm_plex', $f_snpset_id);
    $irods->add_object_avu($snpset_path, 'reference_name', $f_reference_name);
    # write JSON config file with test params
    my %params = (
        "irods_data_path"      => $irods_tmp_coll,
        "platform"             => "fluidigm",
        "reference_name"       => $f_reference_name,
        "reference_path"       => $irods_tmp_coll,
        "snpset_name"          => $f_snpset_id,
    );
    my $params_path_fluidigm = $tmp."/".$f_params_name;
    open my $out, ">", $params_path_fluidigm ||
        $log->logcroak("Cannot open test parameter path '",
                       $params_path_fluidigm, "'");
    print $out to_json(\%params);
    close $out ||
        $log->logcroak("Cannot close test parameter path '",
                       $params_path_fluidigm, "'");
}

sub setup_sequenom {
    # add some dummy sequenom CSV files to the temporary collection
    # add sample and snpset names to metadata
    my $snpset_v1 = "1.0";
    my $snpset_v2 = "2.0";
    # upload regular and alternate-snp input files to iRODS
    for (my $i=0;$i<@sequenom_csv;$i++) {
        my $input = $sequenom_csv[$i];
        my $ipath = $irods_tmp_coll."/".$input;
        $irods->add_object($data_path."/".$input, $ipath);
        $irods->add_object_avu($ipath,'dcterms:identifier', $sample_ids[$i]);
        $irods->add_object_avu($ipath, 'sequenom_plex', $s_snpset_id);
    }
    # add snpset (version "1.0")
    my $snpset_1 = $irods_tmp_coll."/".$s_snpset_filename_1;
    $irods->add_object($data_path."/".$s_snpset_filename_1, $snpset_1);
    $irods->add_object_avu($snpset_1, 'chromosome_json', $cjson_irods);
    $irods->add_object_avu($snpset_1, 'sequenom_plex', $s_snpset_id);
    $irods->add_object_avu($snpset_1, 'reference_name', $s_reference_name);
    $irods->add_object_avu($snpset_1,'snpset_version', $snpset_v1);
    # add snpset (version "2.0")
    my $snpset_path = $irods_tmp_coll."/".$s_snpset_filename;
    $irods->add_object($data_path."/".$s_snpset_filename, $snpset_path);
    $irods->add_object_avu($snpset_path, 'chromosome_json', $cjson_irods);
    $irods->add_object_avu($snpset_path, 'sequenom_plex', $s_snpset_id);
    $irods->add_object_avu($snpset_path, 'reference_name', $s_reference_name);
    $irods->add_object_avu($snpset_path, 'snpset_version', $snpset_v2);
    # write JSON config file with test params
    my %params = (
        irods_data_path      => $irods_tmp_coll,
        platform             => "sequenom",
        reference_name       => $s_reference_name,
        reference_path       => $irods_tmp_coll,
        snpset_name          => $s_snpset_id,
        read_snpset_version  => $snpset_v2,
        write_snpset_version => $snpset_v2,
    );
    my $config_path = $tmp."/".$s_params_name;
    my $out;
    open $out, ">", $config_path ||
        $log->logcroak("Cannot open config file '", $config_path, "'");
    print $out to_json(\%params);
    close $out ||
        $log->logcroak("Cannot close config file '", $config_path, "'");
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}



sub test_command_illuminus : Test(1) {

    my $dbfile = $data_path."/small_test.db";

    my $manifest = "/nfs/gapi/data/genotype/pipeline_test/Human670-QuadCustom_v1_A.bpm.csv";

    my $plex_manifest = $data_path."/foo.tsv";

    my $script = "ready_workflow.pl";

    my $cmd = join q{ }, "$script",
                         "--dbfile $dbfile",
                         "--manifest $manifest",
                         "--run run1",
                         "--verbose",
                         "--plex_manifest $plex_manifest",
                         "--workdir $tmp/genotype_workdir",
                         "--workflow illuminus";


    is(0, system($cmd), "script exit status is zero");

}


return 1;
