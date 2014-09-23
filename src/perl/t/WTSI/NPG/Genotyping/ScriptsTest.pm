
use utf8;

package WTSI::NPG::Genotyping::ScriptsTest;

use strict;
use warnings;
use File::Compare;
use File::Temp qw(tempdir);
use Log::Log4perl;
use JSON;

use base qw(Test::Class);
use Test::More tests => 29;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $PUBLISH_SNPSET              = './bin/publish_snpset.pl';
our $PUBLISH_FLUIDIGM_GENOTYPES  = './bin/publish_fluidigm_genotypes.pl';

our $PUBLISH_INFINIUM_GENOTYPES  = './bin/publish_infinium_genotypes.pl';
our $UPDATE_INFINIUM_METADATA    = './bin/update_infinium_metadata.pl';
our $PUBLISH_INFINIUM_ANALYSIS   = './bin/publish_infinium_analysis.pl';

our $PUBLISH_EXPRESSION_ANALYSIS = './bin/publish_expression_analysis.pl';

our $READY_PIPE     = './bin/ready_pipe.pl';
our $READY_INFINIUM = './bin/ready_infinium.pl';
our $READY_SAMPLES  = './bin/ready_samples.pl';

my $data_path = './t/scripts';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods_tmp_coll = $irods->add_collection("ScriptsTest.$pid");
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods->remove_collection($irods_tmp_coll);
}

sub test_publish_snpset : Test(1) {
  my $data_file = "$data_path/publish_snpset/qc.csv";

  my $reference_name  = 'Homo_sapiens (1000Genomes)';
  my $snpset_name     = 'qc';
  my $snpset_platform = 'sequenom';

  ok(system(join q{ }, "$PUBLISH_SNPSET",
            "--dest $irods_tmp_coll",
            "--reference-name '$reference_name'",
            "--snpset-name $snpset_name",
            "--snpset-platform $snpset_platform",
            "--source $data_file") == 0, 'Published SNPSet');
}

sub test_publish_fluidigm_genotypes : Test(2) {
  my $raw_data_path = "$data_path/publish_fluidigm_genotypes";

  my $snpset_file     = "$raw_data_path/qc.csv";
  my $reference_name  = 'Homo_sapiens (1000Genomes)';
  my $snpset_name     = 'qc';
  my $snpset_platform = 'fluidigm';

  ok(system(join q{ }, "$PUBLISH_SNPSET",
            "--dest $irods_tmp_coll",
            "--reference-name '$reference_name'",
            "--snpset-name $snpset_name",
            "--snpset-platform $snpset_platform",
            "--source $snpset_file") == 0, 'Published SNPSet');

  # Includes a directory with a missing CSV file to check that the
  # script exits successfully when ths happens.
  ok(system(join q{ }, "$PUBLISH_FLUIDIGM_GENOTYPES",
            "--days-ago 0",
            "--days 1000000",
            "--dest $irods_tmp_coll",
            "--reference-path $irods_tmp_coll",
            "--source $raw_data_path",
            "2>/dev/null") == 0,
     'Published Fluidigm genotypes');
}

sub test_publish_infinium_genotypes : Test(3) {
  my $gtc_path  = "$data_path/publish_infinium_genotypes/coreex_bbgahs/gtc";
  my $idat_path = "$data_path/publish_infinium_genotypes/coreex_bbgahs/idat";
  my $raw_data_list = "$data_path/publish_infinium_genotypes/coreex_bbgahs.txt";

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--days-ago 0",
            "--days 1",
            "--project foo",
            "--dest $irods_tmp_coll",
            "2>/dev/null") != 0, '--project conflicts with --days');

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--days-ago 0",
            "--days 0",
            "2>/dev/null") != 0, 'Requires --dest');

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--dest $irods_tmp_coll",
            "- < $raw_data_list") == 0,
     'Published Infinium genotypes from a file list');
}

sub test_update_infinium_metadata : Test(2) {
  my $gtc_path  = "$data_path/publish_infinium_genotypes/coreex_bbgahs/gtc";
  my $idat_path = "$data_path/publish_infinium_genotypes/coreex_bbgahs/idat";
  my $raw_data_list = "$data_path/publish_infinium_genotypes/coreex_bbgahs.txt";

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--dest $irods_tmp_coll",
            "- < $raw_data_list") == 0,
     'Published Infinium genotypes from a file list');

  ok(system(join q{ }, "$UPDATE_INFINIUM_METADATA",
            "--dest $irods_tmp_coll") == 0, 'Updated Infinium metadata');
}

sub test_publish_infinium_analysis : Test(7) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_publish_infinium_analysis.db";

  my $gtc_path  = "$data_path/publish_infinium_analysis/coreex_bbgahs/gtc";
  my $idat_path = "$data_path/publish_infinium_analysis/coreex_bbgahs/idat";
  my $raw_data_list = "$data_path/publish_infinium_analysis/coreex_bbgahs.txt";

  my $analysis_path = "$data_path/publish_infinium_analysis/analysis";

  my $selected_samples_file =
    "$data_path/publish_infinium_analysis/samples_to_include.txt";

  my $archive_coll = "$irods_tmp_coll/infinium";
  my $analysis_coll = "$irods_tmp_coll/analysis";

  my $run         = 'test';
  my $supplier    = 'wtsi';
  my $project     = 'coreex_bbgahs';
  my $qc_platform = 'Sequenom';

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--dest $archive_coll",
            "-", "<", "$raw_data_list") == 0,
     'Published Infinium genotypes from a file list');

  ok(system(join q{ }, "$UPDATE_INFINIUM_METADATA",
            "--dest $archive_coll") == 0, 'Updated Infinium metadata');

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile",
            "--run $run",
            "--supplier $supplier",
            "--project '$project'") == 0, 'Ready infinium');

  # Exclude all samples
  ok(system(join q{ }, "$READY_SAMPLES",
            "--dbfile $dbfile",
            "--select autocall_pass",
            "|",
            "$READY_SAMPLES",
            "--dbfile $dbfile",
            "--add excluded") == 0, 'Excluded all samples');

  # Restore only samples that have test data in iRODS
  ok(system(join q{ }, "$READY_SAMPLES",
            "--dbfile $dbfile",
            "--input $selected_samples_file",
            "--remove excluded") == 0, 'Restored some samples');

  ok(system(join q{ }, "$PUBLISH_INFINIUM_ANALYSIS",
            "--dbfile $dbfile",
            "--source $analysis_path",
            "--dest $analysis_coll",
            "--archive $archive_coll",
            "--run $run") == 0, 'Published analysis');
}

sub test_ready_pipe : Test(2) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_pipe.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);
  ok(-e "$dbfile");
}

sub test_ready_infinium : Test(8) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_infinium.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);

  my $run         = 'test';
  my $supplier    = 'wtsi';
  my $project     = 'coreex_bbgahs';
  my $qc_platform = 'Sequenom';

  ok(system("$READY_INFINIUM 2>/dev/null") != 0, 'Requires --dbfile');

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile",
            "--supplier $supplier",
            "--project '$project'",
            "2>/dev/null") != 0, 'Requires --run');

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile",
            "--run $run",
            "--project '$project'",
            "2>/dev/null") != 0, 'Requires --supplier');

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile ",
            "--run $run",
            "--supplier $supplier",
            "2>/dev/null") != 0, 'Requires --project');

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile",
            "--run $run",
            "--supplier $supplier",
            "--project '$project'",
            "--qc-platform $qc_platform") == 0, 'Basic use');
  ok(-e "$dbfile");
}

sub test_ready_samples : Test(3) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_samples.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);

  my $run         = 'test';
  my $supplier    = 'wtsi';
  my $project     = 'coreex_bbgahs';
  my $qc_platform = 'Sequenom';

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile",
            "--run $run",
            "--supplier $supplier",
            "--project '$project'") == 0);

  ok(system("$READY_SAMPLES --dbfile $dbfile") == 0, 'List samples');
}

sub test_publish_expression_analysis : Test(1) {

  my $idat_path = "$data_path/publish_expression_analysis/infinium";
  my $analysis_path = "$data_path/publish_expression_analysis/results";
  my $manifest_path = "$data_path/publish_expression_analysis/manifest";

  my $archive_coll = "$irods_tmp_coll/infinium";
  my $analysis_coll = "$irods_tmp_coll/analysis";

  ok(system(join q{ }, "$PUBLISH_EXPRESSION_ANALYSIS",
            "--analysis-source $analysis_path",
            "--sample-source $idat_path",
            "--analysis-dest $analysis_coll",
            "--sample-dest $archive_coll",
            "--manifest $manifest_path/hipsci_12samples_2014-02-12.txt",
            "2>/dev/null") == 0, 'Published expression analysis');
}

1;
