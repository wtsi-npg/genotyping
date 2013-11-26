
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::ResultSetTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 15;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::ResultSet'); }

use WTSI::NPG::Genotyping::Fluidigm::ResultSet;

my $result_dir = './t/fluidigm_resultset/complete/0123456789';

my $missing_data = './t/fluidigm_resultset/missing_data/0123456789';
my $missing_export = './t/fluidigm_resultset/missing_export/0123456789';
my $missing_tif = './t/fluidigm_resultset/missing_tif/0123456789';
my $missing_dir = './t/fluidigm_noplace';

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::ResultSet');
}

sub constructor : Test(5) {
  new_ok('WTSI::NPG::Genotyping::Fluidigm::ResultSet',
         [directory => $result_dir]);

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
      (directory => $missing_dir) }
    "Expected to fail when the directory does not exist";

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
      (directory => $missing_data) }
    "Expected to fail when the Data directory does not exist";

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
      (directory => $missing_export) }
    "Expected to fail when the export file does not exist";

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
      (directory => $missing_tif) }
    "Expected to fail when a tif file does not exist";
}

sub paths : Test(8) {
  my $result_set = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
    (directory => $result_dir);

  is($result_set->directory, $result_dir, "Directory is '$result_dir'");
  ok(-d $result_set->directory, 'Directory exists');

  is($result_set->data_directory, "$result_dir/Data",
     "data_directory is 'Data/$result_dir'");
  ok(-d $result_set->data_directory, 'Data directory exists');

  is($result_set->export_file, "$result_dir/0123456789.csv",
     "export_file is '$result_dir'");
  ok(-f $result_set->export_file, 'Export file exists');

  is($result_set->fluidigm_barcode, '0123456789',
     "fluidigm_barcode is '0123456789'");

  is_deeply($result_set->tif_files, ["$result_dir/Data/aramis.tif",
                                     "$result_dir/Data/athos.tif",
                                     "$result_dir/Data/porthos.tif"]) or
     diag explain $result_set->tif_files;
}
