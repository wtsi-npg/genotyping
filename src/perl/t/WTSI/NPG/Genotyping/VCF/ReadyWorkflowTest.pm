use utf8;

package WTSI::NPG::Genotyping::VCF::ReadyWorkflowTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Cwd qw/abs_path/;
use Test::More tests => 50;
use Test::Exception;
use File::Basename qw(fileparse);
use File::Path qw/make_path/;
use File::Slurp qw/read_file/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw/tempdir/;
use JSON;
use Log::Log4perl;
use WTSI::NPG::iRODS;
use YAML qw/LoadFile/;

use WTSI::NPG::Genotyping::VCF::PlexResultFinder;

our $LOG_TEST_CONF =  './etc/log4perl_tests.conf';

Log::Log4perl::init($LOG_TEST_CONF);

# test for ready_qc_calls.pl and ready_workflow.pm
# TODO Later merge this into ScriptsTest.pm, but keep separate for now for quicker testing in development (running ScriptsTest.pm takes ~11 minutes!)

our $READY_QC_CALLS = './bin/ready_qc_calls.pl';
our $READY_WORKFLOW = './bin/ready_workflow.pl';

my $irods;
my $irods_tmp_coll;
my $pid = $$;
my $data_path = abs_path('./t/vcf');
my $tmp;

my $db_file_name = "4_samples.db";
my $dbfile = catfile($data_path, $db_file_name);

my $manifest = catfile($ENV{'GENOTYPE_TEST_DATA'},
                       "Human670-QuadCustom_v1_A.bpm.csv");
my $egt = catfile($ENV{'GENOTYPE_TEST_DATA'},
                  "Human670-QuadCustom_v1_A.egt");

# fluidigm test data
my $f_expected_vcf = $data_path."/fluidigm.vcf";
my $f_reference_name = "Homo_sapiens (1000Genomes)";
my $f_snpset_id = 'qc';
my $f_snpset_filename = 'qc_fluidigm_snp_info_GRCh37.tsv';
my @f_input_files = qw(fluidigm_001.csv fluidigm_002.csv
                       fluidigm_003.csv fluidigm_004.csv);
my $f_sample_json = $data_path."/fluidigm_samples.json";
my $f_params_name = "params_fluidigm.json";

# sequenom test data
my $s_expected_vcf = $data_path."/sequenom.vcf";
my $s_reference_name = "Homo_sapiens (1000Genomes)";
my $s_snpset_id = 'W30467';
my $s_snpset_filename = 'W30467_snp_set_info_GRCh37.tsv';
my $s_snpset_filename_1 = 'W30467_snp_set_info_GRCh37_1.tsv';
my $s_sample_json = $data_path."/sequenom_samples.json";
my $s_params_name = "params_sequenom.json";
my $s_params_name_1 = "params_sequenom_1.json";

my @sample_ids = qw(urn:wtsi:plate0001_A01_sample000001
                    urn:wtsi:plate0001_B01_sample000002
                    urn:wtsi:plate0001_C01_sample000003
                    urn:wtsi:plate0001_D01_sample000004);
my $chromosome_json_filename = "chromosome_lengths_GRCh37.json";
my $cjson_irods;

my $log = Log::Log4perl->get_logger();

my $tfc = 0; # text fixture count


sub require : Test(1) {
    require_ok('WTSI::NPG::Genotyping::VCF::PlexResultFinder');
}

sub construct : Test(1) {

    new_ok('WTSI::NPG::Genotyping::VCF::PlexResultFinder',
           [irods => $irods,
            sample_ids => ['sample_1', 'sample_2']]);

}

sub make_fixture : Test(setup) {
    $tmp = tempdir("ready_plex_test_XXXXXX", CLEANUP => 1);
    $log->info("Created temporary directory $tmp");
    $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = $irods->add_collection("ReadyPlexCallsTest.$pid.$tfc");
    $tfc++;
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
}

sub setup_fluidigm {
    # add some dummy fluidigm CSV files to the temporary collection
    # add sample and snpset names to metadata
    for (my $i=0;$i<@f_input_files;$i++) {
        my $input = $f_input_files[$i];
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
    return $params_path_fluidigm;
}

sub setup_sequenom_alternate {
    my @s_input_files = qw(sequenom_alternate_snp_001.csv
                           sequenom_alternate_snp_002.csv
                           sequenom_alternate_snp_003.csv
                           sequenom_alternate_snp_004.csv);
    my ($default_config, $alternate_config) = setup_sequenom(\@s_input_files);
    return $alternate_config;
}

sub setup_sequenom_default {
    my @s_input_files = qw(sequenom_001.csv
                           sequenom_002.csv
                           sequenom_003.csv
                           sequenom_004.csv);
    my ($default_config, $alternate_config) = setup_sequenom(\@s_input_files);
    return $default_config;
}

sub setup_sequenom {
    # add some dummy sequenom CSV files to the temporary collection
    # add sample and snpset names to metadata
    my @s_input_files = @{$_[0]};
    my $snpset_v1 = "1.0";
    my $snpset_v2 = "2.0";
    # upload regular and alternate-snp input files to iRODS
    for (my $i=0;$i<@s_input_files;$i++) {
        my $input = $s_input_files[$i];
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
    # write another JSON config file with alternate input snpset
    $params{'read_snpset_version'} = $snpset_v1;
    my $config_path_1 = $tmp."/".$s_params_name_1;
    open $out, ">", $config_path_1 ||
        $log->logcroak("Cannot open config file '", $config_path_1, "'");
    print $out to_json(\%params);
    close $out ||
        $log->logcroak("Cannot close config file '", $config_path_1, "'");
    return ($config_path, $config_path_1);
}

sub setup_chromosome_json {
    # upload chromosome json file to temporary irods collection
    # can only upload once per collection
    # must upload before running setup_fluidigm or setup_sequenom
    my $cjson = $data_path."/".$chromosome_json_filename;
    $irods->add_object($cjson, $cjson_irods);
}

sub teardown : Test(teardown) {
    $irods->remove_collection($irods_tmp_coll);
}

sub test_ready_calls_fluidigm : Test(2) {
    setup_chromosome_json();
    my $fluidigm_params = setup_fluidigm();
    my $vcf_out = "$tmp/fluidigm_qc.vcf";
    my $cmd = join q{ }, "$READY_QC_CALLS",
                         "--config $fluidigm_params",
                         "--dbfile $dbfile",
                         "--logconf $LOG_TEST_CONF",
                         "--verbose",
                         "--out $tmp";
    ok(system($cmd) == 0, 'Wrote Fluidigm calls to VCF');
    my @got_lines = read_file($vcf_out);
    @got_lines = grep !/^[#]{2}(fileDate|reference)=/, @got_lines;
    my @expected_lines = read_file($f_expected_vcf);
    @expected_lines = grep !/^[#]{2}(fileDate|reference)=/, @expected_lines;
    is_deeply(\@got_lines, \@expected_lines,
              "Fluidigm VCF output matches expected values");

}

sub test_ready_calls_sequenom : Test(2) {
    setup_chromosome_json();
    my $sequenom_params = setup_sequenom_default();
    my $vcf_out = "$tmp/sequenom_W30467.vcf";
    my $cmd = join q{ }, "$READY_QC_CALLS",
                         "--config $sequenom_params",
                         "--dbfile $dbfile",
                         "--logconf $LOG_TEST_CONF",
                         "--out $tmp";
    ok(system($cmd) == 0, 'Wrote Sequenom calls to VCF');
    my @got_lines = read_file($vcf_out);
    @got_lines = grep !/^[#]{2}(fileDate|reference)=/, @got_lines;
    my @expected_lines = read_file($s_expected_vcf);
    @expected_lines = grep !/^[#]{2}(fileDate|reference)=/, @expected_lines;
    is_deeply(\@got_lines, \@expected_lines,
              "Sequenom VCF output matches expected values");

}

sub test_ready_calls_sequenom_alternate_snp : Test(2) {
    # tests handling of renamed SNP in different manifest versions
    setup_chromosome_json();
    my $sequenom_params = setup_sequenom_alternate();
    my $vcf_out = "$tmp/sequenom_W30467.vcf";
    my $cmd = join q{ }, "$READY_QC_CALLS",
                         "--config $sequenom_params",
                         "--dbfile $dbfile",
                         "--logconf $LOG_TEST_CONF",
                         "--out $tmp";
    ok(system($cmd) == 0, 'Wrote Sequenom calls to VCF');
    my @got_lines = read_file($vcf_out);
    @got_lines = grep !/^[#]{2}(fileDate|reference)=/, @got_lines;
    my @expected_lines = read_file($s_expected_vcf);
    @expected_lines = grep !/^[#]{2}(fileDate|reference)=/, @expected_lines;
    is_deeply(\@got_lines, \@expected_lines,
              "Sequenom VCF output matches expected values");

}

sub test_ready_calls_both : Test(3) {
    # test ready calls script with *both* sequenom and fluidigm specified
    setup_chromosome_json();
    my $fluidigm_params = setup_fluidigm();
    my $sequenom_params = setup_sequenom_default();
    my $fluidigm_out = catfile($tmp, "fluidigm_qc.vcf");
    my $sequenom_out = catfile($tmp, "sequenom_W30467.vcf");
    my $cmd = join q{ }, "$READY_QC_CALLS",
                         "--config $fluidigm_params,$sequenom_params",
                         "--dbfile $dbfile",
                         "--logconf $LOG_TEST_CONF",
                         "--out $tmp";
    ok(system($cmd) == 0, 'Wrote Sequenom and Fluidigm calls to VCF');
    my @got_f = read_file($fluidigm_out);
    @got_f = grep !/^[#]{2}(fileDate|reference)=/, @got_f;
    my @expected_f = read_file($f_expected_vcf);
    @expected_f = grep !/^[#]{2}(fileDate|reference)=/, @expected_f;
    is_deeply(\@got_f, \@expected_f,
              "Fluidigm VCF output matches expected values");
    my @got_s = read_file($sequenom_out);
    @got_s = grep !/^[#]{2}(fileDate|reference)=/, @got_s;
    my @expected_s = read_file($s_expected_vcf);
    @expected_s = grep !/^[#]{2}(fileDate|reference)=/, @expected_s;
    is_deeply(\@got_s, \@expected_s,
              "Sequenom VCF output matches expected values");

}

sub test_result_finder : Test(7) {
    setup_chromosome_json();
    setup_fluidigm();
    setup_sequenom_default();

    # test for single query
    my $finder = WTSI::NPG::Genotyping::VCF::PlexResultFinder->new(
        irods      => $irods,
        sample_ids => \@sample_ids
    );
    my $params_f = {
        irods_data_path      => $irods_tmp_coll,
        platform             => "fluidigm",
        reference_name       => $f_reference_name,
        reference_path       => $irods_tmp_coll,
        snpset_name          => $f_snpset_id,
    };
    my $out_fluidigm_0 = "$tmp/class_test_fluidigm_0.vcf";
    my $total = $finder->read_write_single($params_f,
                                           $out_fluidigm_0,
                                           'class_test_fluidigm');
    ok($total==4, "Results found for 4 Fluidigm samples");
    ok(-e $out_fluidigm_0, "Fluidigm output found");

    # test for more than one query
    my $snpset_v2 = '2.0';
    my $params_s = {
        irods_data_path      => $irods_tmp_coll,
        platform             => "sequenom",
        reference_name       => $s_reference_name,
        reference_path       => $irods_tmp_coll,
        snpset_name          => $s_snpset_id,
        read_snpset_version  => $snpset_v2,
        write_snpset_version => $snpset_v2,
    };

    my $paths = $finder->read_write_all([$params_f, $params_s], $tmp);
    my $f_out_vcf = "$tmp/fluidigm_qc.vcf";
    my $s_out_vcf = "$tmp/sequenom_W30467.vcf";
    my $out_paths = [$f_out_vcf, $s_out_vcf];
    is_deeply($paths, $out_paths, "Fluidigm & Sequenom outputs returned");
    ok(-e $f_out_vcf, "Fluidigm output found");
    ok(-e $s_out_vcf, "Sequenom output found");

    my @got_f = read_file($f_out_vcf);
    @got_f = grep !/^[#]{2}(fileDate|reference)=/, @got_f;
    my @expected_f = read_file($f_expected_vcf);
    @expected_f = grep !/^[#]{2}(fileDate|reference)=/, @expected_f;
    is_deeply(\@got_f, \@expected_f,
              "Fluidigm VCF output matches expected values");
    my @got_s = read_file($s_out_vcf);
    @got_s = grep !/^[#]{2}(fileDate|reference)=/, @got_s;
    my @expected_s = read_file($s_expected_vcf);
    @expected_s = grep !/^[#]{2}(fileDate|reference)=/, @expected_s;
    is_deeply(\@got_s, \@expected_s,
              "Sequenom VCF output matches expected values");
}

sub test_workflow_script_illuminus: Test(16) {
    setup_chromosome_json();
    my $f_config = setup_fluidigm();
    my $s_config = setup_sequenom_default();
    my $plex_manifest_fluidigm = catfile($data_path, $f_snpset_filename);
    my $plex_manifest_sequenom = catfile($data_path, $s_snpset_filename);
    my $workdir = abs_path(catfile($tmp, "genotype_workdir_illuminus"));
    my $config_path = catfile($workdir, "config.yml");
    my $working_db = catfile($workdir, $db_file_name);
    my $cmd = join q{ }, "$READY_WORKFLOW",
                         "--dbfile $dbfile",
                         "--manifest $manifest",
                         "--run run1",
                         "--verbose",
                         "--plex_config $f_config",
                         "--plex_config $s_config",
                         "--plex_manifest $plex_manifest_fluidigm",
                         "--plex_manifest $plex_manifest_sequenom",
                         "--workdir $workdir",
                         "--workflow illuminus";
    is(0, system($cmd), "illuminus setup exit status is zero");
    # check presence of required files and subfolders for workflow
    ok(-e $workdir, "Workflow directory found");
    ok(-e $config_path, "config.yml found");
    ok(-e $working_db, "genotyping SQLite database found");
    foreach my $name (qw/in pass fail/) {
        my $subdir = catfile($workdir, $name);
        ok(-e $subdir && -d $subdir, "Subdirectory '$name' found");
    }
    my $params_path = catfile($workdir, "in", "genotype_illuminus.yml");
    ok(-e $params_path, "genotype_illuminus.yml found");
    my $vcf_path_fluidigm = catfile($workdir, 'vcf', 'fluidigm_qc.vcf');
    my $vcf_path_sequenom = catfile($workdir, 'vcf', 'sequenom_W30467.vcf');
    ok(-e $vcf_path_fluidigm, "Fluidigm VCF file found for Illuminus");

    my $got_fluidigm = _read_without_filedate($vcf_path_fluidigm);
    my $expected_fluidigm_path = catfile($data_path, 'fluidigm.vcf');
    my $expected_fluidigm = _read_without_filedate($expected_fluidigm_path);
    is_deeply($got_fluidigm, $expected_fluidigm,
              "Fluidigm VCF matches expected values");
    ok(-e $vcf_path_sequenom, "Sequenom VCF file found for Illuminus");
    my $got_sequenom = _read_without_filedate($vcf_path_sequenom);
    my $expected_sequenom_path = catfile($data_path, 'sequenom.vcf');
    my $expected_sequenom = _read_without_filedate($expected_sequenom_path);
    is_deeply($got_sequenom, $expected_sequenom,
              "Sequenom VCF matches expected values");
    # check contents of YML files
    my $config = LoadFile($config_path);
    ok($config, "Config data structure loaded from YML");
    my $expected_config =  {
          'msg_port' => '11300',
          'max_processes' => '250',
          'root_dir' => $workdir,
          'log_level' => 'DEBUG',
          'async' => 'lsf',
          'msg_host' => 'farm3-head2',
          'log' => catfile($workdir, 'percolate.log')
        };
    is_deeply($config, $expected_config,
              "YML Illuminus config matches expected values");

    my $params = LoadFile($params_path);
    ok($params, "Workflow parameter data structure loaded from YML");
    my $manifest_name = fileparse($manifest);
    my $fluidigm_manifest_name = fileparse($plex_manifest_fluidigm);
    my $sequenom_manifest_name = fileparse($plex_manifest_sequenom);
    my $expected_params = {
        'workflow' => 'Genotyping::Workflows::GenotypeIlluminus',
        'library' => 'genotyping',
        'arguments' => [
            $working_db,
            'run1',
            $workdir,
            {
                'memory' => '2048',
                'manifest' => catfile($workdir, $manifest_name),
                'chunk_size' => '4000',
                'plex_manifest' => [
                    catfile($workdir, $fluidigm_manifest_name),
                    catfile($workdir, $sequenom_manifest_name),
                ],
                'vcf' => [
                    $vcf_path_fluidigm,
                    $vcf_path_sequenom,
                ],
                'gender_method' => 'Supplied'
            }
        ]
    };
    is_deeply($params, $expected_params,
              "YML Illuminus workflow params match expected values");
}

sub test_workflow_script_zcall: Test(16) {
    setup_chromosome_json();
    my $f_config = setup_fluidigm();
    my $s_config = setup_sequenom_default();
    my $plex_manifest_fluidigm = catfile($data_path, $f_snpset_filename);
    my $plex_manifest_sequenom = catfile($data_path, $s_snpset_filename);
    my $workdir = abs_path(catfile($tmp, "genotype_workdir_zcall"));
    my $working_db = catfile($workdir, $db_file_name);
    my $params_path = catfile($workdir, "in", "genotype_zcall.yml");
    my $cmd = join q{ }, "$READY_WORKFLOW",
                         "--dbfile $dbfile",
                         "--manifest $manifest",
                         "--run run1",
                         "--verbose",
                         "--plex_config $f_config",
                         "--plex_config $s_config",
                         "--plex_manifest $plex_manifest_fluidigm",
                         "--plex_manifest $plex_manifest_sequenom",
                         "--egt $egt",
                         "--zstart 6",
                         "--ztotal 3",
                         "--workdir $workdir",
                         "--workflow zcall";
    is(0, system($cmd), "zcall setup exit status is zero");
    ok(-e $workdir, "Workflow directory found");
    my $config_path = catfile($workdir, "config.yml");
    ok(-e $config_path, "config.yml found");
    ok(-e $working_db, "genotyping SQLite DB found");
    foreach my $name (qw/in pass fail/) {
        my $subdir = catfile($workdir, $name);
        ok(-e $subdir && -d $subdir, "Subdirectory '$name' found");
    }
    ok(-e $params_path, "genotype_zcall.yml found");
    my $vcf_path_fluidigm = catfile($workdir, 'vcf', 'fluidigm_qc.vcf');
    my $vcf_path_sequenom = catfile($workdir, 'vcf', 'sequenom_W30467.vcf');
    ok(-e $vcf_path_fluidigm, "Fluidigm VCF file found for zCall");
    my $got_fluidigm = _read_without_filedate($vcf_path_fluidigm);
    my $expected_fluidigm_path = catfile($data_path, 'fluidigm.vcf');
    my $expected_fluidigm = _read_without_filedate($expected_fluidigm_path);
    is_deeply($got_fluidigm, $expected_fluidigm,
              "Fluidigm VCF matches expected values");
    ok(-e $vcf_path_sequenom, "Sequenom VCF file found for zCall");
    my $got_sequenom = _read_without_filedate($vcf_path_sequenom);
    my $expected_sequenom_path = catfile($data_path, 'sequenom.vcf');
    my $expected_sequenom = _read_without_filedate($expected_sequenom_path);
    is_deeply($got_sequenom, $expected_sequenom,
              "Sequenom VCF matches expected values");
    # check contents of YML files
    my $config = LoadFile($config_path);
    ok($config, "Config data structure loaded from YML");
    my $expected_config =  {
          'msg_port' => '11300',
          'max_processes' => '250',
          'root_dir' => $workdir,
          'log_level' => 'DEBUG',
          'async' => 'lsf',
          'msg_host' => 'farm3-head2',
          'log' => catfile($workdir, 'percolate.log')
        };
    is_deeply($config, $expected_config,
              "YML zCall config matches expected values");
    my $params = LoadFile($params_path);
    ok($params, "Workflow parameter data structure loaded from YML");
    my $manifest_name = fileparse($manifest);
    my $fluidigm_manifest_name = fileparse($plex_manifest_fluidigm);
    my $sequenom_manifest_name = fileparse($plex_manifest_sequenom);
    my $egt_name = fileparse($egt);
    my $expected_params = {
        'workflow' => 'Genotyping::Workflows::GenotypeZCall',
        'library' => 'genotyping',
        'arguments' => [
            $working_db,
            'run1',
            $workdir,
            {
                'zstart' => '6',
                'chunk_size' => '40',
                'egt' => catfile($workdir, $egt_name),
                'vcf' => [
                    $vcf_path_fluidigm,
                    $vcf_path_sequenom,
                ],
                'memory' => '2048',
                'ztotal' => '3',
                'manifest' => catfile($workdir, $manifest_name),
                'plex_manifest' => [
                    catfile($workdir, $fluidigm_manifest_name),
                    catfile($workdir, $sequenom_manifest_name),
                ]
            }
        ]
    };
    is_deeply($params, $expected_params,
              "YML zCall workflow params match expected values");
}


sub _read_without_filedate {
    # read a VCF file, omitting the ##fileDate and ##reference lines
    # Duplicated in VCFTest.pm
    my ($inPath) = @_;
    my $lines = read_file($inPath);
    return _remove_filedate_reference($lines);
}

sub _remove_filedate_reference {
    # remove the fileDate and reference from a string containing VCF
    # return an ArrayRef[Str] containing data
    my ($vcf_str) = @_;
    my @lines_in = split m/\n/msx, $vcf_str;
    my @lines_out;
    foreach my $line (@lines_in) {
        if ( $line =~ /^[#]{2}(fileDate|reference)/msx ) { next; }
        else { push(@lines_out, $line); }
    }
    return \@lines_out;
}

return 1;

