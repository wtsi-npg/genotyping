
use utf8;

package WTSI::NPG::Genotyping::ScriptsTest;

use strict;
use warnings;
use File::Compare;
use File::Temp qw(tempdir);
use Log::Log4perl;
use JSON;

use base qw(WTSI::NPG::Test);
use Test::More tests => 32;
use Test::Exception;

use WTSI::NPG::iRODS;

my $logconf = './etc/log4perl_tests.conf';
Log::Log4perl::init($logconf);

our $PUBLISH_SNPSET              = './bin/publish_snpset.pl';
our $PUBLISH_FLUIDIGM_GENOTYPES  = './bin/publish_fluidigm_genotypes.pl';

our $PUBLISH_INFINIUM_GENOTYPES  = './bin/publish_infinium_genotypes.pl';
our $UPDATE_INFINIUM_METADATA    = './bin/update_infinium_metadata.pl';
our $PUBLISH_INFINIUM_ANALYSIS   = './bin/publish_infinium_analysis.pl';

our $PUBLISH_EXPRESSION_ANALYSIS = './bin/publish_expression_analysis.pl';
our $UPDATE_EXPRESSION_METADATA  = './bin/update_expression_metadata.pl';

our $QUERY_PROJECT_SAMPLES = './bin/query_project_samples.pl';

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
  my $data_file = "$data_path/publish_snpset/qc.tsv";

  my $reference_name  = 'Homo_sapiens (1000Genomes)';
  my $snpset_name     = 'qc';
  my $snpset_platform = 'sequenom';

  ok(system(join q{ }, "$PUBLISH_SNPSET",
            "--dest $irods_tmp_coll",
            "--logconf $logconf",
            "--reference-name '$reference_name'",
            "--snpset-name $snpset_name",
            "--snpset-platform $snpset_platform",
            "--source $data_file") == 0, 'Published SNPSet');
}

sub test_publish_fluidigm_genotypes : Test(2) {
  my $raw_data_path = "$data_path/publish_fluidigm_genotypes";

  my $snpset_file     = "$raw_data_path/qc.tsv";
  my $reference_name  = 'Homo_sapiens (1000Genomes)';
  my $snpset_name     = 'qc';
  my $snpset_platform = 'fluidigm';

  ok(system(join q{ }, "$PUBLISH_SNPSET",
            "--dest $irods_tmp_coll",
            "--logconf $logconf",
            "--reference-name '$reference_name'",
            "--snpset-name $snpset_name",
            "--snpset-platform $snpset_platform",
            "--source $snpset_file") == 0, 'Published SNPSet for Fluidigm');

  # Includes a directory with a missing CSV file to check that the
  # script exits successfully when ths happens.
  ok(system(join q{ }, "$PUBLISH_FLUIDIGM_GENOTYPES",
            "--days-ago 0",
            "--days 1000000",
            "--dest $irods_tmp_coll",
            "--logconf $logconf",
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
            "--logconf $logconf",
            "2>/dev/null") != 0, '--project conflicts with --days');

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--days-ago 0",
            "--days 0",
            "--logconf $logconf",
            "2>/dev/null") != 0, 'Requires --dest');

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--dest $irods_tmp_coll",
            "--logconf $logconf",
            "- < $raw_data_list") == 0,
     'Published Infinium genotypes from a file list');
}

sub test_query_project_samples : Test(2) {

  # add dummy files to iRODS temp collection & update metadata
  my $irods = WTSI::NPG::iRODS->new;
  my $irods_query_coll = $irods_tmp_coll.'/SampleQueryTest';
  my $tempdir = tempdir("SampleQueryTest.$pid.XXXXXX", CLEANUP => 1);
  $irods->add_collection($irods_query_coll);
  my @data_files = qw/9298751015_R01C01_Grn.idat  9298751015_R01C01_Red.idat
                      9298751015_R03C02_Grn.idat  9298751015_R03C02_Red.idat
                      9298751015_R01C01.gtc  9298751015_R03C02.gtc/;
  my $data_path = './t/scripts/query_project_samples/coreex_bbgahs/';
  my $infinium_plate = 'WG0206900-DNA';
  my $beadchip = '9298751015';
  foreach my $file (@data_files) {
    if ($file =~ /\.idat$/) {
      $irods->add_object($data_path.'idat/'.$file, $irods_tmp_coll);
    } elsif ($file =~ /\.gtc$/) {
      $irods->add_object($data_path.'gtc/'.$file, $irods_tmp_coll);
    }
    my $irods_obj = $irods_tmp_coll.'/'.$file;
    $irods->add_object_avu($irods_obj, 'infinium_plate', $infinium_plate);
    $irods->add_object_avu($irods_obj, 'beadchip', $beadchip);
    if ($file =~ /_R01C01/) {
        $irods->add_object_avu($irods_obj, 'infinium_well', 'A01');
        $irods->add_object_avu($irods_obj, 'beadchip_section', 'R01C01');
        $irods->add_object_avu($irods_obj, 'infinium_sample',
                               '285293_A01_SC_SEPI5488306');
    } elsif  ($file =~ /_R03C02/) {
        $irods->add_object_avu($irods_obj, 'infinium_well', 'A02');
        $irods->add_object_avu($irods_obj, 'beadchip_section', 'R03C02');
        $irods->add_object_avu($irods_obj, 'infinium_sample',
                               '285293_A02_SC_SEPI5488315');
    }
  }
  # run script and check exit status
  my $outpath = $tempdir.'/query_results.txt';
  ok(system(join q{ }, "$QUERY_PROJECT_SAMPLES",
            "--project coreex_bbgahs",
            "--limit 2",
            "--logconf $logconf",
            "--header",
            "--root $irods_tmp_coll",
            "--out $outpath") == 0,
     'Query samples in LIMS, iRODS, SequenceScape for given project');
  # validate output
  if (-e $outpath) {
    my $in;
    open $in, "<", $outpath || die "Cannot open '$outpath'";
    my @results = <$in>;
    close $in || die "Cannot close '$outpath'";
    my $expect = './t/scripts/query_project_samples/expected_results.txt';
    open $in, "<", $expect || die "Cannot open '$expect'";
    my @expected = <$in>;
    close $in || die "Cannot close '$expect'";
    is_deeply(\@results, \@expected, "Query results match expected values");
  } else {
    die "Expected output '$outpath' does not exist";
  }
}

sub test_update_infinium_metadata : Test(2) {
  my $gtc_path  = "$data_path/publish_infinium_genotypes/coreex_bbgahs/gtc";
  my $idat_path = "$data_path/publish_infinium_genotypes/coreex_bbgahs/idat";
  my $raw_data_list = "$data_path/publish_infinium_genotypes/coreex_bbgahs.txt";

  ok(system(join q{ }, "$PUBLISH_INFINIUM_GENOTYPES",
            "--dest $irods_tmp_coll",
            "--logconf $logconf",
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
            "--logconf $logconf",
            "-", "<", "$raw_data_list") == 0,
     'Published Infinium genotypes from a file list');

  ok(system(join q{ }, "$UPDATE_INFINIUM_METADATA",
            "--dest $archive_coll") == 0, 'Updated Infinium metadata');

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);

  ok(system(join q{ }, "$READY_INFINIUM",
            "--dbfile $dbfile",            "--run $run",
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
            "--logconf $logconf",
            "--archive $archive_coll",
            "--run $run") == 0, 'Published analysis');
}

sub test_ready_pipe : Test(2) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_pipe.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);
  ok(-e "$dbfile");
}

sub test_ready_infinium : Test(7) {
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
            "--logconf $logconf",
            "--manifest $manifest_path/hipsci_12samples_2014-02-12.txt",
            "2>/dev/null") == 0, 'Published expression analysis');
}

sub test_update_expression_metadata : Test(2) {
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
            "--logconf $logconf",
            "--manifest $manifest_path/hipsci_12samples_2014-02-12.txt",
            "2>/dev/null") == 0, 'Published expression analysis');

  ok(system(join q{ }, "$UPDATE_EXPRESSION_METADATA",
            "--logconf $logconf",
            "--dest $irods_tmp_coll") == 0, 'Updated expression metadata');
}

1;
