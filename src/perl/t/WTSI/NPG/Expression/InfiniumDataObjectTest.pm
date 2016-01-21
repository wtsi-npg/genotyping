
{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database';

  sub find_infinium_gex_sample {
    return {internal_id        => 123456789,
            sanger_sample_id   => '0123456789',
            consent_withdrawn  => 0,
            donor_id           => 'D999',
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
            plate_purpose_name => 'GEX',
            map                => 'A01'};
  }

  sub find_infinium_gex_sample_by_sanger_id {
    my ($self) = @_;
    $self->logconfess("Should not call find_infinium_gex_sample_by_sanger_id");
  }
}

package WTSI::NPG::Expression::InfiniumDataObjectTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use File::Spec;
use Test::More tests => 13;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::InfiniumDataObject'); }

use WTSI::NPG::Expression::InfiniumDataObject;

my $data_path = './t/expression_data_object';
my $idat_file = '012345678901_A_Grn.idat';
my $xml_file = '012345678901_A_Grn.xml';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("ExprInfiniumDataObjectTest.$pid");

  my $idat_irods_path = "$irods_tmp_coll/$idat_file";
  $irods->add_object("$data_path/$idat_file", $idat_irods_path);
  $irods->add_object_avu($idat_irods_path, 'gex_plate', 'plate1');
  $irods->add_object_avu($idat_irods_path, 'gex_well', 'A01');
  $irods->add_object_avu($idat_irods_path, 'type', 'idat');

  my $xml_irods_path = "$irods_tmp_coll/$xml_file";
  $irods->add_object("$data_path/$xml_file", $xml_irods_path);
  $irods->add_object_avu($xml_irods_path, 'gex_plate', 'plate1');
  $irods->add_object_avu($xml_irods_path, 'gex_well', 'A01');
  $irods->add_object_avu($xml_irods_path, 'type', 'xml');

  foreach my $path (($idat_irods_path, $xml_irods_path)) {
    # Add some existing secondary metadata to be superseded
    $irods->add_object_avu($path, 'dcterms:identifier',   '9999999999');
    $irods->add_object_avu($path, 'study_id',             '10');
    $irods->add_object_avu($path, 'sample_consent',       '1');
    $irods->add_object_avu($path, 'sample_supplier_name', 'zzzzzzzzzz');

    # Add some ss_ group permissions to be removed
    $irods->set_object_permissions('read', 'ss_10',  $path);
    $irods->set_object_permissions('read', 'ss_100', $path);
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::InfiniumDataObject');
}

sub metadata : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $idat_irods_path = "$irods_tmp_coll/$idat_file";
  my $xml_irods_path = "$irods_tmp_coll/$xml_file";

  foreach my $path ($idat_irods_path, $xml_irods_path) {
    my $obj = WTSI::NPG::Expression::InfiniumDataObject->new($irods, $path);

    my $infinium_plate = $obj->get_avu('gex_plate');
    is($infinium_plate->{value}, 'plate1', 'Plate metadata is present');

    my $infinium_well = $obj->get_avu('gex_well');
    is($infinium_well->{value}, 'A01', 'Well metadata is present');
  }
}

sub update_secondary_metadata : Test(7) {
  my $irods = WTSI::NPG::iRODS->new;

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $idat_irods_path = "$irods_tmp_coll/$idat_file";
  my $idat_obj = WTSI::NPG::Expression::InfiniumDataObject->new
    ($irods, $idat_irods_path);

  my $expected_groups_before = ['ss_10', 'ss_100'];
  my @groups_before = $idat_obj->get_groups;
  is_deeply(\@groups_before, $expected_groups_before, 'Groups before update')
    or diag explain \@groups_before;

  ok($idat_obj->update_secondary_metadata($ssdb));

  my $idat_expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'gex_plate',               value => 'plate1'},
     {attribute => 'gex_well',                value => 'A01'},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'WTSI'},
     {attribute => 'study_id',                value => '0'},
     {attribute => 'type',                    value => 'idat'}];

  my $idat_meta = [grep { $_->{attribute} !~ m{_history$} }
                   @{$idat_obj->metadata}];
  is_deeply($idat_meta, $idat_expected_meta, 'Secondary metadata added 1')
    or diag explain $idat_meta;

  my $xml_irods_path = "$irods_tmp_coll/$xml_file";
  my $xml_obj = WTSI::NPG::Expression::InfiniumDataObject->new
    ($irods, $xml_irods_path);
  ok($xml_obj->update_secondary_metadata($ssdb));

  my $xml_expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'gex_plate',               value => 'plate1'},
     {attribute => 'gex_well',                value => 'A01'},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'WTSI'},
     {attribute => 'study_id',                value => '0'},
     {attribute => 'type',                    value => 'xml'}];

  my $xml_meta = [grep { $_->{attribute} !~ m{_history$} }
                  @{$xml_obj->metadata}];
  is_deeply($xml_meta, $xml_expected_meta, 'Secondary metadata added 2')
    or diag explain $xml_meta;

  my $expected_groups_after = ['ss_0'];
  foreach my $obj (($idat_obj, $xml_obj)) {
    my @groups_after = $obj->get_groups;

    is_deeply(\@groups_after, $expected_groups_after, 'Groups after update')
      or diag explain \@groups_after;
  }
}

1;
