
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database';

  Log::Log4perl::init('./etc/log4perl_tests.conf');

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
use Test::More tests => 6;
use Test::Exception;

use WTSI::NPG::iRODS qw(add_collection
                        add_object_meta
                        put_collection
                        remove_collection);

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::AssayDataObject'); }

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;

my $data_path = './t/fluidigm_assay_data_object/1381735059';
my $data_file = 'S01_1381735059.csv';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  $irods_tmp_coll = add_collection("FluidigmAssayDataObjectTest.$pid");
  put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/1381735059/$data_file";

  add_object_meta($irods_path, 'fluidigm_plate', '1381735059');
  add_object_meta($irods_path, 'fluidigm_well', 'S01');
};

sub teardown : Test(teardown) {
  remove_collection($irods_tmp_coll);
};

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::AssayDataObject');
};

sub metadata : Test(2) {
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ("$irods_tmp_coll/1381735059/$data_file");

  my (undef, $fluidigm_plate) = $data_object->get_avu('fluidigm_plate');
  is($fluidigm_plate, '1381735059', 'Plate metadata is present');

  my (undef, $fluidigm_well) = $data_object->get_avu('fluidigm_well');
  is($fluidigm_well, 'S01', 'Well metadata is present');

  my (undef, $audience) = $data_object->get_avu('dcterms:audience');
  ok(! defined $audience);
};

sub update_secondary_metadata : Test(2) {
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ("$irods_tmp_coll/1381735059/$data_file");

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  ok($data_object->update_secondary_metadata($ssdb));

  my $expected_meta =
    [['dcterms:identifier'      => '0123456789',   ''],
     ['fluidigm_plate'          => '1381735059',   ''],
     ['fluidigm_well'           => 'S01',          ''],
     ['sample'                  => 'sample1',      ''],
     ['sample_accession_number' => 'A0123456789',  ''],
     ['sample_cohort'           => 'AAA111222333', ''],
     ['sample_common_name'      => 'Homo sapiens', ''],
     ['sample_consent'          => '1',            ''],
     ['sample_control'          => 'XXXYYYZZZ',    ''],
     ['sample_id'               => '123456789',    ''],
     ['sample_supplier_name'    => 'WTSI',         ''],
     ['study_id'                => '0',            '']];

  my $meta = $data_object->metadata;
  is_deeply($meta, $expected_meta, 'Secondary metadata added')
    or diag explain $meta;
}
