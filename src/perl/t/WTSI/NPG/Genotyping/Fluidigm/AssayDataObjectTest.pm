
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use Moose;
  use Carp;

  extends 'WTSI::NPG::Database';

  has 'test_sanger_sample_id' =>
    (is       => 'rw',
     isa      => 'Str | Undef',
     required => 0,
     default  => sub { '0123456789' });

  sub find_fluidigm_sample_by_plate {
    my ($self, $fluidigm_barcode, $well) = @_;

    $well eq 'S01' or
      confess "WarehouseStub expected well argument 'S01' but got '$well'";

    return {internal_id        => 123456789,
            sanger_sample_id   => $self->test_sanger_sample_id,
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
            plate_purpose_name => 'Fluidigm',
            map                => 'S01'};
  }
}

package WTSI::NPG::Genotyping::Fluidigm::AssayDataObjectTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use File::Spec;
use Test::More tests => 11;
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
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods_tmp_coll = $irods->add_collection("FluidigmAssayDataObjectTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/1381735059/$data_file";

  $irods->add_object_avu($irods_path, 'fluidigm_plate', '1381735059');
  $irods->add_object_avu($irods_path, 'fluidigm_well', 'S01');

  # Add some existing secondary metadata to be superseded
  $irods->add_object_avu($irods_path, 'dcterms:identifier',   '9999999999');
  $irods->add_object_avu($irods_path, 'study_id',             '10');
  $irods->add_object_avu($irods_path, 'sample_consent',       '1');
  $irods->add_object_avu($irods_path, 'sample_supplier_name', 'zzzzzzzzzz');


  # Ideally all iRODS groups that are needed for the test should be created
  # at this point and then deleted on exit. As it is, the following iRODS
  # group should be present in test iRODS server before this test is run:
  # ss_0, ss_10, ss_100.

  # Add some ss_ group permissions to be removed
  $irods->set_object_permissions('read', 'ss_10',  $irods_path);
  $irods->set_object_permissions('read', 'ss_100', $irods_path);
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

sub update_secondary_metadata : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'ml_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $expected_groups_before = ['ss_10', 'ss_100'];
  my @groups_before = $data_object->get_groups;
  is_deeply(\@groups_before, $expected_groups_before, 'Groups before update')
    or diag explain \@groups_before;

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
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'aaaaaaaaaa'},
     {attribute => 'study_id',                value => '0'}];

  my $meta = [grep { $_->{attribute} !~ m{_history$} }
              @{$data_object->metadata}];
  is_deeply($meta, $expected_meta, 'Secondary metadata added')
    or diag explain $meta;

  my $expected_groups_after = ['ss_0'];
  my @groups_after = $data_object->get_groups;

  is_deeply(\@groups_after, $expected_groups_after, 'Groups after update')
    or diag explain \@groups_after;
}

sub update_secondary_metadata_missing_value : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'ml_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'),
     test_sanger_sample_id => q{});

  ok($data_object->update_secondary_metadata($ssdb));

  # The (empty) sanger_sample_id gets mapped to dcterms:identifier in
  # the metadata. The attributes are superseded in lexical sort order,
  # so this one is done first. The test ensures that an invalid AVU
  # value only causes that AVU to be skipped - all subsequent ones are
  # applied.
  #
  # In this instance, the update for 'dcterms:identifier' fails because of
  # the invalid value, so the existing 'dcterms:identifier' AVU remains
  # unchanged with a value of '9999999999'. Subsequently, the value of
  # 'sample_supplier_name' is successfully updated from 'zzzzzzzzzz' to
  # 'aaaaaaaaaa'.
  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '9999999999'},
     {attribute => 'fluidigm_plate',          value => '1381735059'},
     {attribute => 'fluidigm_well',           value => 'S01'},
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'aaaaaaaaaa'},
     {attribute => 'study_id',                value => '0'}];

  my $meta = [grep { $_->{attribute} !~ m{_history$} }
              @{$data_object->metadata}];
  is_deeply($meta, $expected_meta,
            'Secondary metadata addition skips bad value')
    or diag explain $meta;
}
