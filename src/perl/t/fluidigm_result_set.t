
# Tests WTSI::NPG::Genotyping::FluidigmExportFile

use utf8;

use strict;
use warnings;

use Test::More tests => 9;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmResultSet'); }
require_ok('WTSI::NPG::Genotyping::FluidigmResultSet');

my $result_dir = './t/fluidigm_result_set';
my $missing_dir = './t/fluidigm_noplace';

ok(WTSI::NPG::Genotyping::FluidigmResultSet->new(directory => $result_dir),
    "Create FluidigmResultSet object");

dies_ok { WTSI::NPG::Genotyping::FluidigmResultSet->new
  (directory => $missing_dir) }
  "Expected to fail when directory argument does not exist";

my $result_set = WTSI::NPG::Genotyping::FluidigmResultSet->new
  (directory => $result_dir);

is($result_set->directory, $result_dir, "directory is '$result_dir'");

is($result_set->data_directory, "$result_dir/Data",
   "data_directory is 'Data/$result_dir'");

is($result_set->export_file, "$result_dir/fluidigm_result_set.csv",
   "export_file is '$result_dir'");

is($result_set->fluidigm_barcode, "fluidigm_result_set",
   "fluidigm_barcode is 'fluidigm_result_set'");

is_deeply($result_set->tif_files, ["$result_dir/Data/aramis.tif",
                                   "$result_dir/Data/athos.tif",
                                   "$result_dir/Data/porthos.tif"]) or
  diag explain $result_set->tif_files;
