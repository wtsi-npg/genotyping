
# Tests generate_yml.pl

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use Test::More tests => 11;
use YAML qw/LoadFile/;

my $temp = tempdir("generate_yml_test_XXXXXX", CLEANUP => 1);
my $wd = "t/genotyping_yml"; # will not write any files here, but directory should exist to avoid warnings
my $manifest = "/nfs/gapi/data/genotype/pipeline_test/Human670-QuadCustom_v1_A.bpm.csv";
my $db = "genotyping_DUMMY.db";

my $config = "config.yml";
my @workflows = qw/null genosnp illuminus zcall/;

my $ref_dir = './t/genotyping_yml/';
my $cmd_root = "genotyping_yml.pl --outdir $temp --run run1 --workdir $wd -dbfile $db";
foreach my $workflow (@workflows) {
    my $cmd;
    if ($workflow eq 'null') { $cmd = $cmd_root; }
    else { $cmd = $cmd_root." --workflow $workflow --manifest $manifest"; }
    if ($workflow eq 'zcall') { $cmd .= ' --egt /home/foo/dummy_cluster.egt'; }
    is(0,system($cmd),"genotyping_yml.pl exit status, $workflow workflow");
    # validate config.yml and workflow file
    my $config = $temp.'/config.yml';
    my $configMaster = $ref_dir.'config.yml';
    is_deeply(LoadFile($config), LoadFile($configMaster), "Config YML data structure equivalent to master");

    # validate the workflow .yml (if any)
    if ($workflow ne 'null') {
	my $output = $temp."/genotype_".$workflow.".yml";
	my $master = $ref_dir."genotype_".$workflow.".yml";
	is_deeply(LoadFile($output), LoadFile($master), "YML data structure equivalent to master, $workflow workflow");
    }
}

