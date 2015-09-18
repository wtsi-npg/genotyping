
use utf8;

package WTSI::NPG::Genotyping::YMLTest;

use strict;
use warnings;
use File::Temp qw/tempdir/;
use YAML qw/LoadFile/;

use base qw(Test::Class);
use Test::More tests => 8;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $dataDir = "/nfs/gapi/data/genotype/pipeline_test/";
my $manifest = $dataDir."Human670-QuadCustom_v1_A.bpm.csv";
my $plex_manifest = $dataDir."W30467_snp_set_info_GRCh37.tsv";
my $egt = $dataDir."Human670-QuadCustom_v1_A.egt";
my $config = "config.yml";
my @workflows = qw/null illuminus zcall/;

sub test_command_line : Test(8) {
    # test exit status and outputs of command line script
    my $temp = tempdir("generate_yml_test_XXXXXX", CLEANUP => 1);
    my $wd = "t/genotyping_yml"; # will not write any files here, but directory should exist to avoid warnings
    my $db = "genotyping_DUMMY.db"; # empty file exists in $wd
    my $ref_dir = './t/genotyping_yml/'; # directory with master files
    my $cmd_root = "genotyping_yml.pl --outdir $temp --run run1 ".
        "--workdir $wd -dbfile $db";
    foreach my $workflow (@workflows) {
	my $cmd;
	if ($workflow eq 'null') { $cmd = $cmd_root; }
	else { $cmd = $cmd_root." --workflow $workflow --manifest $manifest".
                   " --plex_manifest $plex_manifest"; }
	if ($workflow eq 'zcall') { $cmd .= " --egt $egt"; }
	is(0,system($cmd),"genotyping_yml.pl exit status, ".
               $workflow." workflow");
	# validate config.yml and workflow file
	my $configPath = $temp.'/'.$config;
	my $configMaster = $ref_dir.$config;
	is_deeply(LoadFile($configPath), LoadFile($configMaster),
                  "Config YML data structure equivalent to master");

	# validate the workflow .yml (if any)
	if ($workflow ne 'null') {
	    my $output = $temp."/genotype_".$workflow.".yml";
	    my $master = $ref_dir."genotype_".$workflow.".yml";
	    is_deeply(LoadFile($output), LoadFile($master),
                      "YML data structure equivalent to master, ".
                          $workflow." workflow");
	}
    }
}

1;
