
use utf8;

package WTSI::NPG::Genotyping::FluidigmResultSetTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 12;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmResultSet'); }

use WTSI::NPG::Genotyping::FluidigmResultSet;

my $result_dir = './t/fluidigm_result_set';
my $missing_dir = './t/fluidigm_noplace';

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::FluidigmResultSet');
};

sub constructor : Test(2) {
  new_ok('WTSI::NPG::Genotyping::FluidigmResultSet',
         [directory => $result_dir]);

  dies_ok { WTSI::NPG::Genotyping::FluidigmResultSet->new
      (directory => $missing_dir) }
    "Expected to fail when directory argument does not exist";
};

sub paths : Test(8) {
  my $result_set = WTSI::NPG::Genotyping::FluidigmResultSet->new
    (directory => $result_dir);

  is($result_set->directory, $result_dir, "Directory is '$result_dir'");
  ok(-d $result_set->directory, 'Directory exists');

  is($result_set->data_directory, "$result_dir/Data",
     "data_directory is 'Data/$result_dir'");
  ok(-d $result_set->data_directory, 'Data directory exists');

  is($result_set->export_file, "$result_dir/fluidigm_result_set.csv",
     "export_file is '$result_dir'");
  ok(-f $result_set->export_file, 'Export file exists');

  is($result_set->fluidigm_barcode, "fluidigm_result_set",
     "fluidigm_barcode is 'fluidigm_result_set'");

  is_deeply($result_set->tif_files, ["$result_dir/Data/aramis.tif",
                                     "$result_dir/Data/athos.tif",
                                     "$result_dir/Data/porthos.tif"]) or
     diag explain $result_set->tif_files;
};
