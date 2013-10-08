
# Tests WTSI::NPG::Genotyping::FluidigmExportFile

use utf8;

use strict;
use warnings;

use File::Compare;
use File::Temp qw(tempdir);

use Test::More tests => 298;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmExportFile'); }
require_ok('WTSI::NPG::Genotyping::FluidigmExportFile');

my $data_path = './t/fluidigm_export_file';
my $complete_file = "$data_path/complete.csv";
my $header = "$data_path/header.txt";
my $body = "$data_path/body.txt";

ok(WTSI::NPG::Genotyping::FluidigmExportFile->new(file_name => $complete_file));
ok(WTSI::NPG::Genotyping::FluidigmExportFile->new($complete_file));

dies_ok { WTSI::NPG::Genotyping::FluidigmExportFile->new($header) }
  "Expected to fail parsing when body is missing";
dies_ok { WTSI::NPG::Genotyping::FluidigmExportFile->new($body) }
  "Expected to fail parsing when header is missing";

my $export = WTSI::NPG::Genotyping::FluidigmExportFile->new($complete_file);
is($export->fluidigm_barcode, '1381735059', 'Fluidigm barcode differs');
ok($export->confidence_threshold == 65, 'Confidence threshold differs');
ok($export->num_samples == 96, 'Number of samples differs');

# Each sample should have 96 assay results
my @sample_addresses;
for (my $i = 1; $i <= 96; $i++) {
  push(@sample_addresses, sprintf("S%02d", $i));
}
is_deeply($export->sample_addresses, \@sample_addresses,
          'Sample addresses differ');

foreach my $address (@sample_addresses) {
  ok(@{$export->sample_assays($address)} == 96,
     "Sample assay counts differ for sample at $address");
}

my $tmpdir = tempdir(CLEANUP => 1);
foreach my $address (@sample_addresses) {
  my $expected_file = sprintf("%s/%s_%s.csv", $data_path, $address,
                              $export->fluidigm_barcode);
  my $test_file = sprintf("%s/%s_%s.csv", $tmpdir, $address,
                          $export->fluidigm_barcode);

  ok($export->write_sample_assays($address, $test_file) == 96,
     "Failed to write $test_file");

  ok(compare($test_file, $expected_file) == 0,
     "$test_file differs from $expected_file");

  unlink $test_file;
}
