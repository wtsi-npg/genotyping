
# Tests WTSI::NPG::Genotyping::FluidigmExportFile

use utf8;

use strict;
use warnings;

use Test::More tests => 105;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmExportFile'); }
require_ok('WTSI::NPG::Genotyping::FluidigmExportFile');

my $complete_file = './t/fluidigm_export_file/complete.csv';
my $header = './t/fluidigm_export_file/header.txt';
my $body = './t/fluidigm_export_file/body.txt';

ok(WTSI::NPG::Genotyping::FluidigmExportFile->new(file_name => $complete_file));
ok(WTSI::NPG::Genotyping::FluidigmExportFile->new($complete_file));

dies_ok { WTSI::NPG::Genotyping::FluidigmExportFile->new($header) }
  "Expected to fail parsing when body is missing";
dies_ok { WTSI::NPG::Genotyping::FluidigmExportFile->new($body) }
  "Expected to fail parsing when header is missing";

my $export = WTSI::NPG::Genotyping::FluidigmExportFile->new($complete_file);
is($export->fluidigm_barcode, '1381735059');
ok($export->confidence_threshold == 65);
ok($export->num_samples == 96);

# Each sample should have 96 assay results
for (my $i = 1; $i <= 96; $i++) {
  my $address = sprintf("S%02d", $i);
  ok($export->sample_assays($address) == 96);
}
