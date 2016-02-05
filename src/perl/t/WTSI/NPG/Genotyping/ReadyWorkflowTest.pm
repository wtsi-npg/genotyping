use utf8;

package WTSI::NPG::Genotyping::ReadyWorkflowTest;

use strict;
use warnings;
use File::Temp qw/tempdir/;
use YAML qw/LoadFile/;

use base qw(WTSI::NPG::Test);
use Test::More tests => 1;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

sub test_command_illuminus : Test(1) {

    my $data_path = './t/ready_workflow';

    my $tmp = tempdir("ready_workflow_test_XXXXXX", CLEANUP => 0);


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
