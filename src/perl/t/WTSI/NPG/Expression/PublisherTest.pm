
use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database::Warehouse';

  sub find_infinium_gex_sample {
    return {internal_id        => 123456789,
            sanger_sample_id   => 'QC1Hip-86',
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
            plate_purpose_name => 'GEX',
            map                => 'A01'};
  }

  sub find_infinium_gex_sample_by_sanger_id {
    die "Should not call find_infinium_gex_sample_by_sanger_id\n";
  }
}

package WTSI::NPG::Expression::PublisherTest;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;

use base qw(Test::Class);
use Test::More tests => 13;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::Publisher') };

use WTSI::NPG::Expression::ChipLoadingManifestV2;
use WTSI::NPG::Expression::Publisher;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(collect_files
                            collect_dirs
                            modified_between);

my $config = $ENV{HOME} . "/.npg/genotyping.ini";

my $data_path = './t/expression_publisher';
my $manifest_path = "$data_path/manifest.txt";
my @data_files = ("$data_path/0123456789_A_Grn.idat",
                  "$data_path/0123456789_A_Grn.xml",

                  "$data_path/0123456789_B_Grn.idat",
                  "$data_path/0123456789_B_Grn.xml",

                  "$data_path/0123456789_C_Grn.idat",
                  "$data_path/0123456789_C_Grn.xml");

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
    (name => 'sequencescape_warehouse',
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
    (name => 'sequencescape_warehouse',
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

sub publish : Test(4) {
  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Expression::Publisher->new
    (data_files       => [@data_files[0 .. 1]],
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
     [beadchip             => '0123456789'],
     [beadchip_section     => 'A'],
     ['dcterms:identifier' => 'QC1Hip-86'],
     [type                 => 'idat']);
  cmp_ok(scalar @idat_files, '==', 1, 'Number of idat files published');

  my @xml_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '0123456789'],
     [beadchip_section     => 'A'],
     ['dcterms:identifier' => 'QC1Hip-86'],
     [type                 => 'xml']);
  cmp_ok(scalar @xml_files, '==', 1, 'Number of XML files published');
}

sub publish_overwrite : Test(5) {
  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Expression::Publisher->new
    (data_files       => [@data_files[0 .. 1]],
     manifest         => $manifest,
     publication_time => $publication_time,
     sequencescape_db => $ssdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  # Publish
  cmp_ok($publisher->publish($irods_tmp_coll), '==', 2,
         'Number of files published');
  # Publishing again should be a no-op
  cmp_ok($publisher->publish($irods_tmp_coll), '==', 2,
         'Number of files published');

  my $irods = WTSI::NPG::iRODS->new;
  my @idat_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '0123456789'],
     [beadchip_section     => 'A'],
     ['dcterms:identifier' => 'QC1Hip-86'],
     [type                 => 'idat']);
  cmp_ok(scalar @idat_files, '==', 1, 'Number of idat files published');

  my @xml_files = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [beadchip             => '0123456789'],
     [beadchip_section     => 'A'],
     ['dcterms:identifier' => 'QC1Hip-86'],
     [type                 => 'xml']);
  cmp_ok(scalar @xml_files, '==', 1, 'Number of XML files published');
}
