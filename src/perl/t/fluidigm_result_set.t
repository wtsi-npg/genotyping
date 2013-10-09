
# Tests WTSI::NPG::Genotyping::FluidigmExportFile

use utf8;

use strict;
use warnings;

use Test::More tests => 4;
use Test::Exception;

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmResultSet'); }
require_ok('WTSI::NPG::Genotyping::FluidigmResultSet');

my $result_dir = './t/fluidigm_result_set';
my $missing_dir = './t/fluidigm_noplace';

ok(WTSI::NPG::Genotyping::FluidigmResultSet->new(directory => $result_dir),
    "Create FluidigmResultSet object");

dies_ok { WTSI::NPG::Genotyping::FluidigmResultSet->new(directory => $missing_dir) } "Expected to fail when directory argument does not exist"
