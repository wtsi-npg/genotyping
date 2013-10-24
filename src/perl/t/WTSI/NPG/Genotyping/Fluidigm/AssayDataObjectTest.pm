
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayDataObjectTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 4;
use Test::Exception;

use WTSI::NPG::iRODS qw(add_collection
                        add_object_meta
                        put_collection
                        remove_collection);

Log::Log4perl::init('etc/log4perl_tests.conf');

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
  require_ok('WTSI::NPG::Genotyping::FluidigmAssayDataObject');
};

sub metadata : Test(2) {
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ("$irods_tmp_coll/1381735059/$data_file");

  my (undef, $fluidigm_plate) = $data_object->get_avu('fluidigm_plate');
  is($fluidigm_plate, '1381735059', 'Plate metadata is present');

  my (undef, $fluidigm_well) = $data_object->get_avu('fluidigm_well');
  is($fluidigm_well, 'S01', 'Well metadata is present');
};
