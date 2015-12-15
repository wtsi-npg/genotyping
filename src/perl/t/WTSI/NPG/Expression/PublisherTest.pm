
{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database::Warehouse';

  sub find_infinium_gex_sample {
    return {internal_id        => 123456789,
            sanger_sample_id   => 'QC1Hip-88',
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
            barcode            => '294866',
            plate_purpose_name => 'GEX',
            map                => 'C03'};
  }

  sub find_infinium_gex_sample_by_sanger_id {
    return {internal_id        => 123456789,
            sanger_sample_id   => 'QC1Hip-88',
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
            barcode            => '294866',
            plate_purpose_name => 'GEX',
            map                => 'C03'};
  }
}

package WTSI::NPG::Expression::PublisherTest;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;

use base qw(Test::Class);
use Test::More tests => 47;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::Publisher') };

use WTSI::NPG::Expression::ChipLoadingManifestV2;
use WTSI::NPG::Expression::Publisher;
use WTSI::NPG::iRODS;

my $config = $ENV{HOME} . "/.npg/genotyping.ini";

my $data_path = './t/expression_publisher';
my $manifest_path = "$data_path/manifest.txt";
my @data_files = ("$data_path/0123456789_A_Grn.idat",
                  "$data_path/0123456789_A_Grn.xml",

                  "$data_path/0123456789_B_Grn.idat",
                  "$data_path/0123456789_B_Grn.xml",

                  "$data_path/012345678901_C_Grn.idat",
                  "$data_path/012345678901_C_Grn.xml");

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("ExpressionPublisherTest.$pid");
};

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
};

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::Publisher');
};

sub constructor : Test(1) {
  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $publication_time = DateTime->now;
  new_ok('WTSI::NPG::Expression::Publisher',
         [data_files       => \@data_files,
          manifest         => $manifest,
          publication_time => $publication_time,
          sequencescape_db => $ssdb]);
}

sub resultsets : Test(1) {
  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Expression::Publisher->new
    (data_files       => \@data_files,
     manifest         => $manifest,
     publication_time => $publication_time,
     sequencescape_db => $ssdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 3, 'Found resultsets');
}

sub publish : Test(21) {
  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Expression::Publisher->new
    (data_files       => [@data_files[4 .. 5]],
     manifest         => $manifest,
     publication_time => $publication_time,
     sequencescape_db => $ssdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  cmp_ok($publisher->publish($irods_tmp_coll), '==', 2,
         'Number of files published');

  my $irods = WTSI::NPG::iRODS->new;
  my @idat_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '012345678901'],
     [beadchip_section     => 'C'],
     [type                 => 'idat']);
  cmp_ok(scalar @idat_files, '==', 1, 'Number of idat files published');

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => 'QC1Hip-88'},
     {attribute => 'gex_plate',               value => 'DN294866F'}, # manifest
     {attribute => 'gex_well',                value => 'C3'},        # manifest
     {attribute => 'md5',
      value     => 'd41d8cd98f00b204e9800998ecf8427e' },
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

  my $idat_path = $idat_files[0];
  my $is_modified = 0;
  test_metadata($irods, $idat_path, $expected_meta, $is_modified);

  my @xml_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '012345678901'],
     [beadchip_section     => 'C'],
     ['dcterms:identifier' => 'QC1Hip-88'],
     [type                 => 'xml']);
  cmp_ok(scalar @xml_files, '==', 1, 'Number of XML files published');
}

sub publish_overwrite : Test(22) {
  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Expression::Publisher->new
    (data_files       => [@data_files[4 .. 5]],
     manifest         => $manifest,
     publication_time => $publication_time,
     sequencescape_db => $ssdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  # Publish
  cmp_ok($publisher->publish($irods_tmp_coll), '==', 2,
         'Number of files published 1');
  # Publishing again should be a no-op
  cmp_ok($publisher->publish($irods_tmp_coll), '==', 2,
         'Number of files published 2');

  my $irods = WTSI::NPG::iRODS->new;
  my @idat_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '012345678901'],
     [beadchip_section     => 'C'],
     [type                 => 'idat']);
  cmp_ok(scalar @idat_files, '==', 1, 'Number of idat files published');

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => 'QC1Hip-88'},
     {attribute => 'gex_plate',               value => 'DN294866F'}, # manifest
     {attribute => 'gex_well',                value => 'C3'},        # manifest
     {attribute => 'md5',
      value     => 'd41d8cd98f00b204e9800998ecf8427e' },
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

  my $idat_path = $idat_files[0];
  my $is_modified = 0; # Checking that we can republish the same data
  test_metadata($irods, $idat_path, $expected_meta, $is_modified);

  my @xml_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '012345678901'],
     [beadchip_section     => 'C'],
     ['dcterms:identifier' => 'QC1Hip-88'],
     [type                 => 'xml']);
  cmp_ok(scalar @xml_files, '==', 1, 'Number of XML files published');
}

sub test_metadata {
  my ($irods, $data_path, $expected_metadata, $is_modified) = @_;

  my $data_object = WTSI::NPG::iRODS::DataObject->new($irods, $data_path);

  ok($data_object->get_avu('dcterms:created'),  'Has dcterms:created');
  ok($data_object->get_avu('dcterms:creator'),  'Has dcterms:creator');

  if ($is_modified) {
    ok($data_object->get_avu('dcterms:modified'), 'Has dcterms:modified');
  }
  else {
    ok(!$data_object->get_avu('dcterms:modified'), 'Has no dcterms:modified');
  }

  foreach my $avu (@$expected_metadata) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    ok($data_object->find_in_metadata($attr, $value), "Found $attr => $value");
  }
}
