
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResultSetTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 11;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet'); }

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;

my $data_path = './t/fluidigm_assay_data_object/1381735059';
my $data_file = 'S01_1381735059.csv';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("FluidigmAssayResultSetTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/1381735059/$data_file";

  $irods->add_object_avu($irods_path, 'fluidigm_plate', '1381735059');
  $irods->add_object_avu($irods_path, 'fluidigm_well', 'S01');
  $irods->add_object_avu($irods_path, 'fluidigm_plex', 'qc');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet');
}

sub constructor : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  # From file
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         [file_name => "$data_path/$data_file"]);
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         ["$data_path/$data_file"]);

  # From data object
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         [data_object => $data_object]);
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         [$data_object]);

  dies_ok {
    WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
        (file_name   => "$data_path/$data_file",
         data_object => $data_object);
  } 'Cannot construct from both file and data object';
}

sub assay_results : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");
  my $resultset = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
    ($data_object);

  cmp_ok(scalar @{$resultset->assay_results}, '==', 96,
         'Contains expected number of assay results');
}

sub snpset_name : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");
  my $resultset = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
    ($data_object);

  is($resultset->snpset_name, 'qc', 'Correct SNP set name');

  $irods->add_object_avu($resultset->data_object->str,
                         'fluidigm_plex', 'test');
  $resultset->data_object->clear_metadata; # Clear metadata cache

  dies_ok { $resultset->snpset_name }, 'Cannot have multiple names';
}

sub snp_names : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");
  my $resultset = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
    ($data_object);

  my @snp_names = $resultset->snp_names;
  my @expected_names = qw(GS34251
                          GS35220
                          rs11096957
                          rs12828016
                          rs156697
                          rs1801262
                          rs1805034
                          rs1805087
                          rs2247870
                          rs2286963
                          rs3742207
                          rs3795677
                          rs4075254
                          rs4619
                          rs4843075
                          rs5215
                          rs6166
                          rs649058
                          rs6557634
                          rs6759892
                          rs7298565
                          rs753381
                          rs7627615
                          rs8065080);

  is_deeply(\@snp_names, \@expected_names,
            'Contains expected SNP names') or diag explain \@snp_names;
}

1;
