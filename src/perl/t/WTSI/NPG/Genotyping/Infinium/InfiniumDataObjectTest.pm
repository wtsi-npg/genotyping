
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use Carp;
  use Moose;

  extends 'WTSI::NPG::Database';

  sub find_infinium_sample_by_plate {
    my ($self, $infinium_barcode, $map) = @_;

    $map eq 'A10' or
       confess "WarehouseStub expected map argument 'A10' but got '$map'";

    return {internal_id        => 123456789,
            sanger_sample_id   => '0123456789',
            consent_withdrawn  => 0,
            donor_id           => 'D999',
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
            plate_purpose_name => 'Infinium',
            map                => 'A10'};
  }

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

package WTSI::NPG::Genotyping::Infinium::InfiniumDataObjectTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 9;
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
  $irods->add_object_avu($gtc_irods_path, 'infinium_well', 'A10');
  $irods->add_object_avu($gtc_irods_path, 'type', 'gtc');

  # Add some existing secondary metadata to be superseded
  $irods->add_object_avu($gtc_irods_path, 'dcterms:identifier',   '9999999999');
  $irods->add_object_avu($gtc_irods_path, 'study_id',             '10');
  $irods->add_object_avu($gtc_irods_path, 'sample_consent',       '1');
  $irods->add_object_avu($gtc_irods_path, 'sample_supplier_name', 'zzzzzzzzzz');

  # Add some ss_ group permissions to be removed
  $irods->set_object_permissions('read', 'ss_10',  $gtc_irods_path);
  $irods->set_object_permissions('read', 'ss_100', $gtc_irods_path);
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
  is($infinium_well->{value}, 'A10', 'Well metadata is present');
}

sub update_secondary_metadata : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $gtc_irods_path = "$irods_tmp_coll/$gtc_file";
  my $data_object = WTSI::NPG::Genotyping::Infinium::InfiniumDataObject->new
    ($irods, $gtc_irods_path);

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $expected_groups_before = ['ss_10', 'ss_100'];
  my @groups_before = $data_object->get_groups;
  is_deeply(\@groups_before, $expected_groups_before, 'Groups before update')
    or diag explain \@groups_before;

  ok($data_object->update_secondary_metadata($ssdb));

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'infinium_plate',          value => 'plate1'},
     {attribute => 'infinium_well',           value => 'A10'},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'aaaaaaaaaa'},
     {attribute => 'study_id',                value => '0'},
     {attribute => 'type',                    value => 'gtc'}];

  my $meta = $data_object->metadata;
  is_deeply($meta, $expected_meta, 'Secondary metadata superseded')
    or diag explain $meta;

  my $expected_groups_after = ['ss_0'];
  my @groups_after = $data_object->get_groups;

  is_deeply(\@groups_after, $expected_groups_after, 'Groups after update')
    or diag explain \@groups_after;
}
