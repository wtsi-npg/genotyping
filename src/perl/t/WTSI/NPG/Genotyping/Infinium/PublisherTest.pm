
use utf8;

{
  package WTSI::NPG::Genotyping::Database::InfiniumStub;

  use Moose;

  extends 'WTSI::NPG::Genotyping::Database::Infinium';

  has 'test_chip_design' =>
    (is       => 'rw',
     isa      => 'Str',
     required => 0,
     default  => sub { 'design1' });

  my $root = "./t/infinium_publisher";

  sub find_scanned_sample {
    my ($self, $filename) = @_;

    return
      {project           => 'project1',
       plate             => 'plate1',
       well              => 'A10',
       sample            => 'sample1',
       beadchip          => '111345689',
       beadchip_section  => 'R01C01',
       beadchip_design   => $self->test_chip_design,
       beadchip_revision => '1',
       status            => 'Pass',
       gtc_path          => "$root/gtc/0123456789/0123456789_R01C01.gtc",
       idat_grn_path     => "$root/idat/0123456789/0123456789_R01C01_Grn.idat",
       idat_red_path     => "$root/idat/0123456789/0123456789_R01C01_Red.idat"}
  }

  sub find_called_sample {
     my ($self, $filename) = @_;

    return
      {project           => 'project1',
       plate             => 'plate1',
       well              => 'A10',
       sample            => 'sample1',
       beadchip          => '111345689',
       beadchip_section  => 'R01C01',
       beadchip_design   => $self->test_chip_design,
       beadchip_revision => '1',
       status            => 'Pass',
       gtc_path          => "$root/gtc/0123456789/0123456789_R01C01.gtc",
       idat_grn_path     => "$root/idat/0123456789/0123456789_R01C01_Grn.idat",
       idat_red_path     => "$root/idat/0123456789/0123456789_R01C01_Red.idat"}
  }

  sub is_methylation_chip_design {
    my ($self, $chip_design) = @_;

    if ($chip_design eq 'methylation_design1') {
      return 1;
    }
    else {
      return 0;
    }
  }

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

{
  package WTSI::NPG::Database::WarehouseStub;

  use warnings;
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

package WTSI::NPG::Genotyping::Infinium::PublisherTest;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;

use base qw(Test::Class);
use Test::More tests => 338;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Infinium::Publisher') };

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::Genotyping::Infinium::Publisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(collect_files
                            collect_dirs
                            modified_between);

my $config = $ENV{HOME} . "/.npg/genotyping.ini";

my $data_path = './t/infinium_publisher';
my @data_files = ("$data_path/gtc/0123456789/0123456789_R01C01.gtc",
                  "$data_path/idat/0123456789/0123456789_R01C01_Grn.idat",
                  "$data_path/idat/0123456789/0123456789_R01C01_Red.idat",

                  "$data_path/gtc/0123456789/0123456789_R01C02.gtc",
                  "$data_path/idat/0123456789/0123456789_R01C02_Grn.idat",
                  "$data_path/idat/0123456789/0123456789_R01C02_Red.idat",

                  "$data_path/gtc/0123456789/0123456789_R02C02.gtc",
                  "$data_path/idat/0123456789/0123456789_R02C02_Grn.idat",
                  "$data_path/idat/0123456789/0123456789_R02C02_Red.idat",

                  "$data_path/gtc/0123456799/0123456799_R01C01.gtc",
                  "$data_path/idat/0123456799/0123456799_R01C01_Grn.idat",
                  "$data_path/idat/0123456799/0123456799_R01C01_Red.idat",

                  "$data_path/gtc/0123456799/0123456799_R01C02.gtc",
                  "$data_path/idat/0123456799/0123456799_R01C02_Grn.idat",
                  "$data_path/idat/0123456799/0123456799_R01C02_Red.idat",

                  # 11-digit barcode number
                  "$data_path/gtc/01234567999/01234567999_R01C01.gtc",
                  "$data_path/idat/01234567999/01234567999_R01C01_Grn.idat",
                  "$data_path/idat/01234567999/01234567999_R01C01_Red.idat",

                  # 12-digit barcode number
                  "$data_path/gtc/012345679999/012345679999_R01C01.gtc",
                  "$data_path/idat/012345679999/012345679999_R01C01_Grn.idat",
                  "$data_path/idat/012345679999/012345679999_R01C01_Red.idat",

                  "$data_path/gtc/0123456799/0123456799_R01C03.gtc",
                  # Missing Grn idat file
                  # Missing Red idat file

                  "$data_path/gtc/0123456799/0123456799_R02C02.gtc",
                  "$data_path/idat/0123456799/0123456799_R02C02_Grn.idat",
                  "$data_path/idat/0123456799/0123456799_R02C02_Red.idat",

                  # Missing GTC file
                  "$data_path/idat/0123456799/0123456799_R02C04_Grn.idat",
                  "$data_path/idat/0123456799/0123456799_R02C04_Red.idat");

my @methyl_files = ("$data_path/idat/0123456799/0123456799_R02C04_Grn.idat",
                    "$data_path/idat/0123456799/0123456799_R02C04_Red.idat");

my @repub_files =
  ("$data_path/repub/gtc/0123456789/0123456789_R01C01.gtc",
   "$data_path/repub/idat/0123456789/0123456789_R01C01_Grn.idat",
   "$data_path/repub/idat/0123456789/0123456789_R01C01_Red.idat"
);

my $irods_tmp_coll;

my $pid = $$;

# Database handle stubs
my $ifdb;
my $ssdb;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("InfiniumPublisherTest.$pid");

  $ifdb = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name    => 'infinium',
     inifile => $config)->connect(RaiseError => 1);

  $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));
};

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Infinium::Publisher');
}

sub constructor : Test(1) {
  my $publication_time = DateTime->now;

  new_ok('WTSI::NPG::Genotyping::Infinium::Publisher',
         [data_files       => \@data_files,
          infinium_db      => $ifdb,
          ss_warehouse_db  => $ssdb,
          publication_time => $publication_time]);
}

sub resultsets : Test(2) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => \@data_files,
     infinium_db      => $ifdb,
     ss_warehouse_db  => $ssdb,
     publication_time => $publication_time);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 8,
         'Found only complete resultsets 1');

  my $ifdb_mod = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name             => 'infinium',
     test_chip_design => 'methylation_design1',
     inifile          => $config)->connect(RaiseError => 1);

  my $methyl_publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => \@data_files,
    infinium_db      => $ifdb_mod,
    ss_warehouse_db  => $ssdb,
    publication_time => $publication_time);

  cmp_ok(scalar @{$methyl_publisher->resultsets}, '==', 9,
	 'Found only complete resultsets 2');
}

sub publish : Test(67) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => [@data_files[0 .. 2]],
     infinium_db      => $ifdb,
     ss_warehouse_db  => $ssdb,
     publication_time => $publication_time);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  cmp_ok($publisher->publish($irods_tmp_coll), '==', 3,
         'Number of files published');

  my $irods = WTSI::NPG::iRODS->new;
  my @gtc_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                               [infinium_plate => 'plate1'],
                                               [infinium_well  => 'A10'],
                                               [type           => 'gtc']);
  cmp_ok(scalar @gtc_files, '==', 1, 'Number of GTC files published');

  my @idat_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                                [infinium_plate => 'plate1'],
                                                [infinium_well  => 'A10'],
                                                [type           => 'idat']);
  cmp_ok(scalar @idat_files, '==', 2, 'Number of idat files published');

  my $expected_meta =
    [{attribute => 'beadchip',                value => '111345689'},
     {attribute => 'beadchip_design',         value => 'design1'},
     {attribute => 'beadchip_section',        value => 'R01C01'},
     {attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'infinium_plate',          value => 'plate1'},
     {attribute => 'infinium_well',           value => 'A10'},
     {attribute => 'md5',
      value     => 'd41d8cd98f00b204e9800998ecf8427e'}, # MD5 of empty file
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

  foreach my $data_path (@gtc_files, @idat_files) {
    test_metadata($irods, $data_path, $expected_meta);
  }
}

sub publish_longer_barcodes : Test(130) {
  # 11 or 12 digits instead of 10 in barcode
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => [@data_files[15 .. 20]],
     infinium_db      => $ifdb,
     ss_warehouse_db  => $ssdb,
     publication_time => $publication_time);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 2,
         'Number of resultsets prepared for long barcodes');

  cmp_ok($publisher->publish($irods_tmp_coll), '==', 6,
         'Number of files published for long barcodes');

  my $irods = WTSI::NPG::iRODS->new;
  my @gtc_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                               [infinium_plate => 'plate1'],
                                               [infinium_well  => 'A10'],
                                               [type           => 'gtc']);
  cmp_ok(scalar @gtc_files, '==', 2,
         'Number of GTC files published for long barcode');

  my @idat_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                                [infinium_plate => 'plate1'],
                                                [infinium_well  => 'A10'],
                                                [type           => 'idat']);
  cmp_ok(scalar @idat_files, '==', 4,
         'Number of idat files published for long barcode');

  my $expected_meta =
    [{attribute => 'beadchip',                value => '111345689'},
     {attribute => 'beadchip_design',         value => 'design1'},
     {attribute => 'beadchip_section',        value => 'R01C01'},
     {attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'infinium_plate',          value => 'plate1'},
     {attribute => 'infinium_well',           value => 'A10'},
     {attribute => 'md5',
      value     => 'd41d8cd98f00b204e9800998ecf8427e'}, # MD5 of empty file
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

  foreach my $data_path (@gtc_files, @idat_files) {
    test_metadata($irods, $data_path, $expected_meta);
  }
}

sub publish_methylation : Test(45) {
  my $publication_time = DateTime->now;

  my $ifdb_mod = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name             => 'infinium',
     test_chip_design => 'methylation_design1',
     inifile          => $config)->connect(RaiseError => 1);

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => \@methyl_files,
     infinium_db      => $ifdb_mod,
     ss_warehouse_db  => $ssdb,
     publication_time => $publication_time);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  my $methyl_coll = "$irods_tmp_coll/methyl";
  cmp_ok($publisher->publish($methyl_coll), '==', 2,
         'Number of files published');

  my $irods = WTSI::NPG::iRODS->new;
  my @idat_files = $irods->find_objects_by_meta($methyl_coll,
                                                [infinium_plate => 'plate1'],
                                                [infinium_well  => 'A10'],
                                                [type           => 'idat']);
  cmp_ok(scalar @idat_files, '==', 2, 'Number of idat files published');

  my $expected_meta =
    [{attribute => 'beadchip',                value => '111345689'},
     {attribute => 'beadchip_design',         value => 'methylation_design1'},
     {attribute => 'beadchip_section',        value => 'R01C01'},
     {attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'infinium_plate',          value => 'plate1'},
     {attribute => 'infinium_well',           value => 'A10'},
     {attribute => 'md5',
      value     => 'd41d8cd98f00b204e9800998ecf8427e'}, # MD5 of empty file
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

  foreach my $data_path (@idat_files) {
    test_metadata($irods, $data_path, $expected_meta);
  }
}

sub publish_overwrite : Test(91) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => [@data_files[0 .. 2]],
     infinium_db      => $ifdb,
     ss_warehouse_db  => $ssdb,
     publication_time => $publication_time);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  cmp_ok($publisher->publish($irods_tmp_coll), '==', 3,
         'Number of files published');

  my $republisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (data_files       => \@repub_files,
     infinium_db      => $ifdb,
     ss_warehouse_db  => $ssdb,
     publication_time => $publication_time);

  cmp_ok(scalar @{$republisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  # Republish
  cmp_ok($republisher->publish($irods_tmp_coll), '==', 3,
         'Number of files re-published');

  my $irods = WTSI::NPG::iRODS->new;
  my @gtc_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                               [infinium_plate => 'plate1'],
                                               [infinium_well  => 'A10'],
                                               [type           => 'gtc']);
  cmp_ok(scalar @gtc_files, '==', 1, 'Number of GTC files re-published');

  my @idat_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                                [infinium_plate => 'plate1'],
                                                [infinium_well  => 'A10'],
                                                [type           => 'idat']);
  cmp_ok(scalar @idat_files, '==', 2, 'Number of idat files re-published');

  my $expected_meta =
    [{attribute => 'beadchip',                value => '111345689'},
     {attribute => 'beadchip_design',         value => 'design1'},
     {attribute => 'beadchip_section',        value => 'R01C01'},
     {attribute => 'dcterms:identifier',      value => '0123456789'},
     {attribute => 'infinium_plate',          value => 'plate1'},
     {attribute => 'infinium_well',           value => 'A10'},
     {attribute => 'md5',
      value     => '091b1055538afa15681edcdbf0e721c8'}, # MD5 of modified file
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

  foreach my $data_path (@gtc_files, @idat_files) {
    test_metadata($irods, $data_path, $expected_meta, 1);
  }
}

sub test_metadata {
  my ($irods, $data_path, $expected_metadata, $is_modified) = @_;

  my $data_object = WTSI::NPG::iRODS::DataObject->new($irods, $data_path);

  ok($data_object->get_avu('dcterms:created'),  'Has dcterms:created');
  ok($data_object->get_avu('dcterms:creator'),  'Has dcterms:creator');
  ok($data_object->get_avu('type'),             'Has type');

  if ($is_modified) {
    ok($data_object->get_avu('dcterms:modified'), 'Has dcterms:modified');
  }
  else {
    ok(!$data_object->get_avu('dcterms:modified'), 'Has no dcterms:modified');
  }

  foreach my $avu (@$expected_metadata) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    ok($data_object->find_in_metadata($attr, $value),
       "Found $attr => $value") or diag explain $data_object->metadata;
  }
}

1;
