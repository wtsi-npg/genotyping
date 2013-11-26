
use utf8;

{
  package WTSI::NPG::Genotyping::Database::InfiniumStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Genotyping::Database::Infinium';

  my $root = "./t/infinium_publisher";
  my $sample =
    {project           => 'project1',
     plate             => 'plate1',
     well              => 'A01',
     sample            => 'sample1',
     beadchip          => '012345689',
     beadchip_section  => 'R01C01',
     beadchip_design   => 'design1',
     beadchip_revision => '1',
     status            => 'Pass',
     gtc_path          => "$root/gtc/0123456789/0123456789_R01C01.gtc",
     idat_grn_path     => "$root/idat/0123456789/0123456789_R01C01_Grn.idat",
     idat_red_path     => "$root/idat/0123456789/0123456789_R01C01_Red.idat"};

  sub find_scanned_sample {
    return $sample;
  }

  sub find_called_sample {
    return $sample;
  }
}

package WTSI::NPG::Genotyping::Infinium::PublisherTest;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;

use base qw(Test::Class);
use Test::More tests => 12;
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

                  "$data_path/gtc/0123456799/0123456799_R01C03.gtc",
                  # Missing Grn idat file
                  # Missing Red idat file

                  "$data_path/gtc/0123456799/0123456799_R02C02.gtc",
                  "$data_path/idat/0123456799/0123456799_R02C02_Grn.idat",
                  "$data_path/idat/0123456799/0123456799_R02C02_Red.idat",

                  # Missing GTC file
                  "$data_path/idat/0123456799/0123456799_R02C04_Grn.idat",
                  "$data_path/idat/0123456799/0123456799_R02C04_Red.idat");

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("InfiniumPublisherTest.$pid");
};

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
};

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Infinium::Publisher');
};

sub resultsets : Test(1) {
  my $ifdb = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name    => 'infinium',
     inifile => $config)->connect(RaiseError => 1);

  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (publication_time => $publication_time,
     data_files       => \@data_files,
     infinium_db      => $ifdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 6,
         'Found only complete resultsets');
}

sub publish : Test(4) {
  my $ifdb = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name    => 'infinium',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (publication_time => $publication_time,
     data_files       => [@data_files[0 .. 2]],
     infinium_db      => $ifdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  cmp_ok($publisher->publish($irods_tmp_coll), '==', 3,
         'Number of files published');

  my $irods = WTSI::NPG::iRODS->new;
  my @gtc_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                               [infinium_plate => 'plate1'],
                                               [infinium_well  => 'A01'],
                                               [type           => 'gtc']);
  cmp_ok(scalar @gtc_files, '==', 1, 'Number of GTC files published');

  my @idat_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                                [infinium_plate => 'plate1'],
                                                [infinium_well  => 'A01'],
                                                [type           => 'idat']);
  cmp_ok(scalar @idat_files, '==', 2, 'Number of idat files published');
}

sub publish_overwrite : Test(5) {
  my $ifdb = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name    => 'infinium',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (publication_time => $publication_time,
     data_files       => [@data_files[0 .. 2]],
     infinium_db      => $ifdb);

  cmp_ok(scalar @{$publisher->resultsets}, '==', 1,
         'Number of resultsets prepared');

  # Publish
  cmp_ok($publisher->publish($irods_tmp_coll), '==', 3,
         'Number of files published');
  # Publishing again should be a no-op
  cmp_ok($publisher->publish($irods_tmp_coll), '==', 3,
         'Number of files re-published');

  my $irods = WTSI::NPG::iRODS->new;
  my @gtc_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                               [infinium_plate => 'plate1'],
                                               [infinium_well  => 'A01'],
                                               [type           => 'gtc']);
  cmp_ok(scalar @gtc_files, '==', 1, 'Number of GTC files published');

  my @idat_files = $irods->find_objects_by_meta($irods_tmp_coll,
                                                [infinium_plate => 'plate1'],
                                                [infinium_well  => 'A01'],
                                                [type           => 'idat']);
  cmp_ok(scalar @idat_files, '==', 2, 'Number of idat files published');
}

