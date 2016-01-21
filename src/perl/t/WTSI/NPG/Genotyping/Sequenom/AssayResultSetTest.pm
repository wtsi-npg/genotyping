
use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayResultSetTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use File::Spec;
use Test::More tests => 11;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::AssayResultSet'); }

use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;

my $data_path = './t/sequenom_assay_data_object';
my $data_file = 'plate1_A01.csv';
my $irods_tmp_coll;

my $resultset;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SequenomAssayResultSetTest.$pid");
  my $irods_path = "$irods_tmp_coll/$data_file";

  $irods->add_object("$data_path/$data_file", $irods_path);
  $irods->add_object_avu($irods_path, 'sequenom_plate', 'plate1');
  $irods->add_object_avu($irods_path, 'sequenom_well', 'A01');
  $irods->add_object_avu($irods_path, 'sequenom_plex', 'qc');

  # Add some existing secondary metadata to be superseded
  $irods->add_object_avu($irods_path, 'dcterms:identifier',   '9999999999');
  $irods->add_object_avu($irods_path, 'study_id',             '10');
  $irods->add_object_avu($irods_path, 'sample_consent',       '1');
  $irods->add_object_avu($irods_path, 'sample_supplier_name', 'zzzzzzzzzz');

  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
    ($irods, "$irods_tmp_coll/$data_file");
  $resultset = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new
    ($data_object);
}

sub teardown : Test(teardown) {
  undef $resultset;

  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::AssayResultSet');
}

sub constructor : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
    ($irods, "$irods_tmp_coll/$data_file");

  # From file
  new_ok('WTSI::NPG::Genotyping::Sequenom::AssayResultSet',
         [file_name => "$data_path/$data_file"]);
  new_ok('WTSI::NPG::Genotyping::Sequenom::AssayResultSet',
         ["$data_path/$data_file"]);

  # From data object
  new_ok('WTSI::NPG::Genotyping::Sequenom::AssayResultSet',
         [data_object => $data_object]);
  new_ok('WTSI::NPG::Genotyping::Sequenom::AssayResultSet',
         [$data_object]);

  dies_ok {
    WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new
        (file_name   => "$data_path/$data_file",
         data_object => $data_object);
  } 'Cannot construct from both file and data object';
}

sub size : Test(1) {
  cmp_ok($resultset->size, '==', 1, 'Expected size');
}

sub assay_results : Test(1) {
  cmp_ok(scalar @{$resultset->assay_results}, '==', 1,
         'Contains expected number of assay results');
}

sub snpset_name : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  is($resultset->snpset_name, 'qc', 'Correct SNP set name');

  $irods->add_object_avu($resultset->data_object->str,
                         'sequenom_plex', 'test');
  $resultset->data_object->clear_metadata; # Clear metadata cache

  dies_ok { $resultset->snpset_name }, 'Cannot have multiple names';
}

1;
