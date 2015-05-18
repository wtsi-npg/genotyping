
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;
  use Carp;

  use base 'WTSI::NPG::Database';

  sub find_fluidigm_sample_by_plate {
    my ($self, $fluidigm_barcode, $well) = @_;

    $well eq 'S01' or
      confess "WarehouseStub expected well argument 'S01' but got '$well'";

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
            plate_purpose_name => 'Fluidigm',
            map                => 'S01'};
  }
}

package WTSI::NPG::Genotyping::Fluidigm::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 47;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher') };

use WTSI::NPG::Genotyping::Fluidigm::Publisher;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;
use WTSI::NPG::iRODS;

my $data_path = './t/fluidigm_publisher';
my $fluidigm_directory = "$data_path/0123456789";
my $fluidigm_repub_directory = "$data_path/repub/0123456789";
my $snpset_file = 'qc.tsv';

my $resultset;
my $reference_path;
my $irods_tmp_coll;

my $pid = $$;

# Database handle stubs
my $whdb;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods_tmp_coll = "FluidigmPublisherTest.$pid";
  $irods->add_collection($irods_tmp_coll);
  $irods->add_object("$data_path/$snpset_file", "$irods_tmp_coll/$snpset_file");

  $reference_path = WTSI::NPG::iRODS::Collection->new
    ($irods, "$irods_tmp_coll" )->absolute->str;

  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/$snpset_file" )->absolute;
  $snpset_obj->add_avu('fluidigm_plex', 'qc');
  $snpset_obj->add_avu('reference_name', 'Homo_sapiens (1000Genomes)');

  $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
    (directory => $fluidigm_directory);

  $whdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);

  undef $resultset;
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher');
}

sub constructor : Test(1) {
  my $publication_time = DateTime->now;

  new_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher',
         [publication_time => $publication_time,
          resultset        => $resultset,
          reference_path   => $reference_path,
          warehouse_db     => $whdb]);
}

sub publish : Test(21) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset,
     reference_path   => $reference_path,
     warehouse_db     => $whdb);

  my @addresses_to_publish = qw(S01);
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', scalar @addresses_to_publish,
         "Number of chunks published");

  my $irods = WTSI::NPG::iRODS->new;
  my $audience = 'http://psd-production.internal.sanger.ac.uk';
  my @aggregate_data =
     $irods->find_objects_by_meta($irods_tmp_coll,
                                  [fluidigm_plate     => '1381735059'],
                                  ['dcterms:audience' => "$audience%", 'like']);
  cmp_ok(scalar @aggregate_data, '==', 1,
         "Number of aggregate data published with dcterms:audience metadata");

  my @chunked_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate => '1381735059'],
                                 [fluidigm_well  => 'S01'],
                                 [fluidigm_plex  => 'qc']);
  cmp_ok(scalar @chunked_data, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");

  my @chunked_audience =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate     => '1381735059'],
                                 [fluidigm_well      => 'S01'],
                                 [fluidigm_plex      => 'qc'],
                                 ['dcterms:audience' => "$audience%", 'like']);
  cmp_ok(scalar @chunked_audience, '==', 0,
         "Number of chunks published with dcterms:audience metadata");

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'fluidigm_plate',          value => '1381735059'},
     {attribute => 'fluidigm_well',           value => 'S01'},
     {attribute => 'md5',
      value     => 'f5f1aaa74edd3167a95b9081ebcc3a40' },
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

  my $data_path = $chunked_data[0];
  test_metadata($irods, $data_path, $expected_meta);
}

sub publish_overwrite : Test(21) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset,
     reference_path   => $reference_path,
     warehouse_db     => $whdb);

  my $repub_resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
    (directory => $fluidigm_repub_directory);
  my $republisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $repub_resultset,
     reference_path   => $reference_path,
     warehouse_db     => $whdb);

  my @addresses_to_publish = qw(S01);

  # First publish
  cmp_ok($publisher->publish($irods_tmp_coll, @addresses_to_publish), '==', 1,
         'Number of wells published 1');

  # Now re-publish with no changes to the data.
  cmp_ok($publisher->publish($irods_tmp_coll, @addresses_to_publish), '==', 1,
         'Number of wells re-published 1');

  # Finally, re-publish with changes to the data.
  cmp_ok($republisher->publish($irods_tmp_coll, @addresses_to_publish), '==', 1,
         'Number of wells re-published 2');

  my $irods = WTSI::NPG::iRODS->new;
  my @republished_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate => '1381735059'],
                                 [fluidigm_well  => 'S01'],
                                 [fluidigm_plex  => 'qc']);
  cmp_ok(scalar @republished_data, '==', scalar @addresses_to_publish,
         "Number of wells re-published with fluidigm_plate metadata");

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'fluidigm_plate',          value => '1381735059'},
     {attribute => 'fluidigm_well',           value => 'S01'},
     {attribute => 'md5',
      value     => 'ac6f6e164a64a21ad8ec379b6d9226e6' },
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

  my $data_path = $republished_data[0];
  test_metadata($irods, $data_path, $expected_meta, 1);
}

sub publish_ambiguous_snpset : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  # Add a new copy of the SNP set containing the same SNPS, but having
  # a distinct name
  $irods->add_object("$data_path/$snpset_file",
                     "$irods_tmp_coll/$snpset_file.2");
  my $snpset_copy = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file.2")->absolute;
  $snpset_copy->add_avu('fluidigm_plex', 'qc2');
  $snpset_copy->add_avu('reference_name', 'Homo_sapiens (1000Genomes)');

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset,
     reference_path   => $reference_path,
     warehouse_db     => $whdb);

  my @addresses_to_publish = qw(S01);
  ok(!$publisher->publish($irods_tmp_coll, @addresses_to_publish),
     'Failed to publish with ambiguous SNP set data');
}

sub publish_ambiguous_metadata : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  # Add a new fluidigm_plex name to the existing SNP set
  my $snpset = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file")->absolute;
  $snpset->add_avu('fluidigm_plex', 'qc2');

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset,
     reference_path   => $reference_path,
     warehouse_db     => $whdb);

  my @addresses_to_publish = qw(S01);
  ok(!$publisher->publish($irods_tmp_coll, @addresses_to_publish),
     'Failed to publish with abmiguous SNP set metadata');
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

1;
