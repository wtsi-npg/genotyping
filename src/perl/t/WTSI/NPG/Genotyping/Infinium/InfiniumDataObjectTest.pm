
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database';

  sub find_infinium_sample_by_plate {
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
            plate_purpose_name => 'Infinium',
            map                => 'A01'};
  }
}

package WTSI::NPG::Genotyping::Infinium::InfiniumDataObjectTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 7;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Infinium::InfiniumDataObject'); }

use WTSI::NPG::Genotyping::Infinium::InfiniumDataObject;

my $data_path = './t/infinium_data_object';
my $gtc_file = '0123456789_R01C01.gtc';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("InfiniumDataObjectTest.$pid");

  my $gtc_irods_path = "$irods_tmp_coll/$gtc_file";
  $irods->add_object("$data_path/$gtc_file", $gtc_irods_path);

  $irods->add_object_avu($gtc_irods_path, 'infinium_plate', 'plate1');
  $irods->add_object_avu($gtc_irods_path, 'infinium_well', 'A01');
  $irods->add_object_avu($gtc_irods_path, 'type', 'gtc');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Infinium::InfiniumDataObject');
}

sub metadata : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;

  my $gtc_irods_path = "$irods_tmp_coll/$gtc_file";
  my $data_object = WTSI::NPG::Genotyping::Infinium::InfiniumDataObject->new
    ($irods, $gtc_irods_path);

  my $infinium_plate = $data_object->get_avu('infinium_plate');
  is($infinium_plate->{value}, 'plate1', 'Plate metadata is present');

  my $infinium_well = $data_object->get_avu('infinium_well');
  is($infinium_well->{value}, 'A01', 'Well metadata is present');
}

sub update_secondary_metadata : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $gtc_irods_path = "$irods_tmp_coll/$gtc_file";
  my $data_object = WTSI::NPG::Genotyping::Infinium::InfiniumDataObject->new
    ($irods, $gtc_irods_path);

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  ok($data_object->update_secondary_metadata($ssdb));

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'infinium_plate',          value => 'plate1'},
     {attribute => 'infinium_well',           value => 'A01'},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'WTSI'},
     {attribute => 'study_id',                value => '0'},
     {attribute => 'type',                    value => 'gtc'}];

  my $meta = $data_object->metadata;
  is_deeply($meta, $expected_meta, 'Secondary metadata added')
    or diag explain $meta;
}
