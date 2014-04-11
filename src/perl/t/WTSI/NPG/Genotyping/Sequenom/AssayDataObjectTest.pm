
use utf8;

{
  package WTSI::NPG::Genotyping::Database::SNPStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Genotyping::Database::SNP';

  Log::Log4perl::init('./etc/log4perl_tests.conf');

  sub find_sequenom_plate_id {
    return 123456789;
  }

  sub find_plate_status {
    return 'Genotyping Done';
  }

  sub find_well_status {
    return 'OK';
  }
}

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;
  use Carp;

  use base 'WTSI::NPG::Database';

  Log::Log4perl::init('./etc/log4perl_tests.conf');

  sub find_sample_by_plate {
    my ($self, $plate_id, $map) = @_;
    $plate_id == 123456789 or confess
       confess "WarehouseStub expected plate_id argument '123456789' " .
         "but got '$plate_id'";
    $map eq 'A10' or
      confess "WarehouseStub expected map argument 'A10' but got '$map'";

    return {internal_id        => 123456789,
            sanger_sample_id   => '0123456789',
            consent_withdrawn  => 0,
            uuid               => 'AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDD',
            name               => 'sample1',
            common_name        => 'Homo sapiens',
            supplier_name      => 'aaaaaaaaaa',
            accession_number   => 'A0123456789',
            gender             => 'Female',
            cohort             => 'AAA111222333',
            control            => 'XXXYYYZZZ',
            study_id           => 0,
            barcode_prefix     => 'DN',
            barcode            => '0987654321',
            plate_purpose_name => 'Sequenom',
            map                => 'A10'};
  }
}

package WTSI::NPG::Genotyping::Sequenom::AssayDataObjectTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 6;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::AssayDataObject'); }

use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;

my $data_path = './t/sequenom_assay_data_object';
my $data_file = 'plate1_A01.csv';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SequenomAssayDataObjectTest.$pid");
  my $irods_path = "$irods_tmp_coll/$data_file";

  $irods->add_object("$data_path/$data_file", $irods_path);
  $irods->add_object_avu($irods_path, 'sequenom_plate', 'plate1');
  $irods->add_object_avu($irods_path, 'sequenom_well', 'A10');

  # Add some existing secondary metadata to be superseded
  $irods->add_object_avu($irods_path, 'dcterms:identifier',   '9999999999');
  $irods->add_object_avu($irods_path, 'study_id',             '10');
  $irods->add_object_avu($irods_path, 'sample_consent',       '1');
  $irods->add_object_avu($irods_path, 'sample_supplier_name', 'zzzzzzzzzz');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::AssayDataObject');
}

sub metadata : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
    ($irods, "$irods_tmp_coll/$data_file");

  my $sequenom_plate = $data_object->get_avu('sequenom_plate');
  is($sequenom_plate->{value}, 'plate1', 'Plate metadata is present');

  my $sequenom_well = $data_object->get_avu('sequenom_well');
  is($sequenom_well->{value}, 'A10', 'Well metadata is present');
}

sub update_secondary_metadata : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
    ($irods, "$irods_tmp_coll/$data_file");

  my $snpdb = WTSI::NPG::Genotyping::Database::SNPStub->new
    (name => 'snp',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  ok($data_object->update_secondary_metadata($snpdb, $ssdb));

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'manual_qc',               value => 1},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'aaaaaaaaaa'},
     {attribute => 'sequenom_plate',          value => 'plate1'},
     {attribute => 'sequenom_well',           value => 'A10'},
     {attribute => 'study_id',                value => '0'}];

  my $meta = $data_object->metadata;
  is_deeply($meta, $expected_meta, 'Secondary metadata superseded')
    or diag explain $meta;
}
