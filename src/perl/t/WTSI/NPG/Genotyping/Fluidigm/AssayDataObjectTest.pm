
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database';

  sub find_fluidigm_sample_by_plate {
    return {internal_id        => 123456789,
            sanger_sample_id   => '0123456789',
            consent_withdrawn  => 0,
            uuid               => 'AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDD',
            name               => 'sample1',
            common_name        => 'Homo sapiens',
            supplier_name      => 'WTSI',
            accession_number   => 'A0123456789',
            gender             => 'Female',
            cohort             => 'AAA111222333',
            control            => 'XXXYYYZZZ',
            study_id           => 0,
            barcode_prefix     => 'DN',
            barcode            => '0987654321',
            plate_purpose_name => 'Fluidigm',
            map                => 'S01'};
  }
}

package WTSI::NPG::Genotyping::Fluidigm::AssayDataObjectTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 9;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::AssayDataObject'); }

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;

my $data_path = './t/fluidigm_assay_data_object/1381735059';
my $data_file = 'S01_1381735059.csv';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("FluidigmAssayDataObjectTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/1381735059/$data_file";

  $irods->add_object_avu($irods_path, 'fluidigm_plate', '1381735059');
  $irods->add_object_avu($irods_path, 'fluidigm_well', 'S01');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::AssayDataObject');
}

sub metadata : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  my $fluidigm_plate = $data_object->get_avu('fluidigm_plate');
  is($fluidigm_plate->{value}, '1381735059', 'Plate metadata is present');

  my $fluidigm_well = $data_object->get_avu('fluidigm_well');
  is($fluidigm_well->{value}, 'S01', 'Well metadata is present');

  my $audience = $data_object->get_avu('dcterms:audience');
  ok(! defined $audience);
}

sub update_secondary_metadata : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  ok($data_object->update_secondary_metadata($ssdb));

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'fluidigm_plate',          value => '1381735059'},
     {attribute => 'fluidigm_well',           value => 'S01'},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'WTSI'},
     {attribute => 'study_id',                value => '0'}];

  my $meta = $data_object->metadata;
  is_deeply($meta, $expected_meta, 'Secondary metadata added')
    or diag explain $meta;
}

sub assay_resultset : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  my $resultset = $data_object->assay_resultset;

  ok($resultset, 'Assay resultset');
  cmp_ok(scalar @{$resultset->assay_results}, '==', 96,
         'Contains expected number of assay results');
}
