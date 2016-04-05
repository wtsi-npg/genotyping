
use utf8;

{
  package WTSI::NPG::Genotyping::Database::SequenomStub;

  use Carp;
  use Moose;

  has 'test_genotype' =>
    (is       => 'rw',
     isa      => 'Str',
     required => 0,
     default  => sub { 'CT' });

  sub find_plate_results {
    my ($self, $plate_name) = @_;

    $plate_name eq 'plate1' or confess
      "SequenomStub expected plate name argument 'plate1' " .
        " but got '$plate_name'";

    return {'A10' => [{customer   => 'customer1',
                       project    => 'project1',
                       plate      => 'plate1',
                       experiment => 'experiment1',
                       chip       => 1234,
                       well       => 'A10',
                       assay      => 'assay1-rs012345678',
                       genotype   => $self->test_genotype,
                       status     => 'status1',
                       sample     => 'sample1',
                       allele     => 'C',
                       mass       => 1,
                       height     => 10},
                      {customer   => 'customer1',
                       project    => 'project1',
                       plate      => 'plate1',
                       experiment => 'experiment1',
                       chip       => 1234,
                       well       => 'A10',
                       assay      => 'assay1-rs987654321',
                       genotype   => $self->test_genotype,
                       status     => 'status1',
                       sample     => 'sample1',
                       allele     => 'C',
                       mass       => 1,
                       height     => 10}]};
  }

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

{
  package WTSI::NPG::Genotyping::Database::SNPStub;

  use Moose;

  extends 'WTSI::NPG::Genotyping::Database::SNP';

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

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

{
  package WTSI::NPG::Database::WarehouseStub;

  use Carp;
  use Moose;

  extends 'WTSI::NPG::Database';

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
            plate_purpose_name => 'Sequenom',
            map                => 'A10'};
  }

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

package WTSI::NPG::Genotyping::Sequenom::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(WTSI::NPG::Test);
use Test::More tests => 46;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::Publisher') };

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Sequenom::Publisher;
use WTSI::NPG::iRODS;

my $irods_tmp_coll;

my $pid = $$;

# Database handle stubs
my $sqdb;
my $snpdb;
my $ssdb;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = "SequenomPublisherTest.$pid";
  $irods->add_collection($irods_tmp_coll);

  $sqdb = WTSI::NPG::Genotyping::Database::SequenomStub->new
    (name    => 'mspec2',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  $snpdb = WTSI::NPG::Genotyping::Database::SNPStub->new
    (name    => 'snp',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::Publisher');
}

sub constructor : Test(1) {
  my $publication_time = DateTime->now;
  my $plate_name = 'plate1';

  new_ok('WTSI::NPG::Genotyping::Sequenom::Publisher',
         [publication_time => $publication_time,
          plate_name       => $plate_name,
          sequenom_db      => $sqdb,
          snp_db           => $snpdb,
          ss_warehouse_db  => $ssdb]);
};

sub publish : Test(21) {
  my $publication_time = DateTime->now;
  my $plate_name = 'plate1';

  my $publisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
    (publication_time => $publication_time,
     plate_name       => $plate_name,
     sequenom_db      => $sqdb,
     snp_db           => $snpdb,
     ss_warehouse_db  => $ssdb);

  my @addresses_to_publish = qw(A10);
  cmp_ok($publisher->publish($irods_tmp_coll, @addresses_to_publish), '==', 1,
         'Number of wells published');

  my $irods = WTSI::NPG::iRODS->new;
  my @published_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [sequenom_plate => 'plate1'],
                                 [sequenom_well  => 'A10'],
                                 [sequenom_plex  => 'assay1']);
  cmp_ok(scalar @published_data, '==', scalar @addresses_to_publish,
         "Number of wells published with sequenom_plate metadata");

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'md5',
      value     => '79e26200a53c5e6b4a3ffb873c1dd753' },
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'aaaaaaaaaa'},
     {attribute => 'sequenom_plate',          value => 'plate1'},
     {attribute => 'sequenom_plex',           value => 'assay1'},
     {attribute => 'sequenom_well',           value => 'A10'},
     {attribute => 'study_id',                value => '0'},
     {attribute => 'type',                    value => 'csv'}];

  my $data_path = $published_data[0];
  test_metadata($irods, $data_path, $expected_meta);
}

sub publish_overwrite : Test(22) {
  my $publication_time = DateTime->now;
  my $plate_name = 'plate1';

  my $publisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
    (publication_time => $publication_time,
     plate_name       => $plate_name,
     sequenom_db      => $sqdb,
     snp_db           => $snpdb,
     ss_warehouse_db  => $ssdb);

  my @addresses_to_publish = qw(A10);
  cmp_ok($publisher->publish($irods_tmp_coll, @addresses_to_publish), '==', 1,
         'Number of wells published');

  my $mod_sqdb = WTSI::NPG::Genotyping::Database::SequenomStub->new
    (name          => 'mspec2',
     inifile       => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'),
     test_genotype => 'CC');

  my $republisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
    (publication_time => $publication_time,
     plate_name       => $plate_name,
     sequenom_db      => $mod_sqdb,
     snp_db           => $snpdb,
     ss_warehouse_db  => $ssdb);

  cmp_ok($republisher->publish($irods_tmp_coll, @addresses_to_publish), '==', 1,
         'Number of wells re-published');

  my $irods = WTSI::NPG::iRODS->new;
  my @republished_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [sequenom_plate => 'plate1'],
                                 [sequenom_well  => 'A10'],
                                 [sequenom_plex  => 'assay1'] );
  cmp_ok(scalar @republished_data, '==', scalar @addresses_to_publish,
         "Number of wells re-published with sequenom_plate metadata");

  my $expected_meta =
    [{attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'md5',
      value     => '8080411ee1f2d4ad9aed273f0b9d3d7e' },
     {attribute => 'sample',                  value => 'sample1' },
     {attribute => 'sample_accession_number', value => 'A0123456789'},
     {attribute => 'sample_cohort',           value => 'AAA111222333'},
     {attribute => 'sample_common_name',      value => 'Homo sapiens'},
     {attribute => 'sample_consent',          value => '1'},
     {attribute => 'sample_control',          value => 'XXXYYYZZZ'},
     {attribute => 'sample_donor_id',         value => 'D999'},
     {attribute => 'sample_id',               value => '123456789'},
     {attribute => 'sample_supplier_name',    value => 'aaaaaaaaaa'},
     {attribute => 'sequenom_plate',          value => 'plate1'},
     {attribute => 'sequenom_plex',           value => 'assay1'},
     {attribute => 'sequenom_well',           value => 'A10'},
     {attribute => 'study_id',                value => '0'},
     {attribute => 'type',                    value => 'csv'}];

  my $data_path = $republished_data[0];
  test_metadata($irods, $data_path, $expected_meta, 1);
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
    my @exists = $data_object->find_in_metadata('dcterms:modified');
    ok(scalar(@exists)==0, 'Has no dcterms:modified');
  }

  foreach my $avu (@$expected_metadata) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    ok($data_object->find_in_metadata($attr, $value), "Found $attr => $value");
  }
}

1;
