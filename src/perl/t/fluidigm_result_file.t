
# Tests WTSI::NPG::Genotyping::FluidigmResultFile

use utf8;

use strict;
use warnings;

use Test::More tests => 9;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmResultFile'); }
require_ok('WTSI::NPG::Genotyping::FluidigmResultFile');

my $complete_file = './t/fluidigm_result_file/complete.csv';
my $header = './t/fluidigm_result_file/header.txt';
my $body = './t/fluidigm_result_file/body.txt';

ok(WTSI::NPG::Genotyping::FluidigmResultFile->new(file_name => $complete_file));
ok(WTSI::NPG::Genotyping::FluidigmResultFile->new($complete_file));

dies_ok { WTSI::NPG::Genotyping::FluidigmResultFile->new($header) }
  "Expected to fail parsing when body is missing";
dies_ok { WTSI::NPG::Genotyping::FluidigmResultFile->new($body) }
  "Expected to fail parsing when header is missing";

my $result = WTSI::NPG::Genotyping::FluidigmResultFile->new($complete_file);
is($result->fluidigm_barcode, '1381735059');
ok($result->confidence_threshold == 65);
ok($result->num_samples == 96);
