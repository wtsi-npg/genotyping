package WTSI::NPG::Genotyping::QC_wip::Check::IdentityScriptTest;

use strict;
use warnings;
use Carp;
use File::Temp qw(tempdir);
use File::Slurp qw(read_file);
use JSON;

use base qw(Test::Class);
use Test::More tests => 2;
use Test::Exception;

my $pid = $$;
my $data_dir = "./t/qc/check/identity/";

sub script : Test(2) {
    # test of the new identity command-line script

    my $identity_script_wip = "./bin/check_identity_bed_wip.pl";
    my $tempdir = tempdir("IdentityTest.$pid.XXXXXX", CLEANUP => 1);
    my $outPath = "$tempdir/identity.json";
    my $plexDir = "/nfs/srpipe_references/genotypes";
    my $plexFile = "$plexDir/W30467_snp_set_info_1000Genomes.tsv";
    my $refPath = "$data_dir/identity_script_output.json";

    ok(system(join q{ }, "$identity_script_wip",
              "--dbpath $data_dir/fake_genotyping.db",
              "--plink $data_dir/fake_qc_genotypes",
              "--out $outPath",
              "--plex_manifest $plexFile",
          ) == 0, 'Completed identity check');

    my $outData = from_json(read_file($outPath));
    my $refData = from_json(read_file($refPath));
    is_deeply($outData, $refData,
              "Identity check JSON output matches reference file");

}
