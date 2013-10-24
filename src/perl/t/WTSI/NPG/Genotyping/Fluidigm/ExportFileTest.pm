
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::ExportFileTest;

use strict;
use warnings;

use File::Compare;
use File::Temp qw(tempdir);

use base qw(Test::Class);
use Test::More tests => 299;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::ExportFile'); }

use WTSI::NPG::Genotyping::Fluidigm::ExportFile;

my $data_path = './t/fluidigm_export_file';
my $complete_file = "$data_path/complete.csv";
my $header = "$data_path/header.txt";
my $body = "$data_path/body.txt";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::ExportFile');
};

sub constructor : Test(5) {
  new_ok('WTSI::NPG::Genotyping::Fluidigm::ExportFile',
         [file_name => $complete_file]);

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
      (file_name => 'no_such_file_exists') }
    "Expected to fail constructing with missing file";

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ExportFile->new() }
    "Expected to fail constructing with no arguments";

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
      (file_name => $header) }
    "Expected to fail parsing when body is missing";

  dies_ok { WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
      (file_name => $body) }
    "Expected to fail parsing when header is missing";
};

sub header_parse : Test(3) {
  my $export = WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
    (file_name => $complete_file);

  is($export->fluidigm_barcode, '1381735059', 'Fluidigm barcode is 1381735059');
  cmp_ok($export->confidence_threshold, '==', 65, 'Confidence threshold == 65');
  cmp_ok($export->size, '==', 96, 'Number of samples == 96');
};

sub sample_assays : Test(97) {
  my $export = WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
    (file_name => $complete_file);

  my @sample_addresses;
  for (my $i = 1; $i <= 96; $i++) {
    push(@sample_addresses, sprintf("S%02d", $i));
  }
  is_deeply($export->addresses, \@sample_addresses,
            'Expected sample addresses') or diag explain $export->addresses;

  # Each sample should have 96 assay results
  foreach my $address (@sample_addresses) {
    cmp_ok(@{$export->sample_assays($address)}, '==', 96,
           "Sample assay count at address $address");
  }
};

sub write_sample_assays : Test(192) {
  my $export = WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
    (file_name => $complete_file);

  my @sample_addresses;
  for (my $i = 1; $i <= 96; $i++) {
    push(@sample_addresses, sprintf("S%02d", $i));
  }

  my $tmpdir = tempdir(CLEANUP => 1);
  foreach my $address (@sample_addresses) {
    my $expected_file = sprintf("%s/%s_%s.csv", $data_path, $address,
                                $export->fluidigm_barcode);
    my $test_file = sprintf("%s/%s_%s.csv", $tmpdir, $address,
                            $export->fluidigm_barcode);

    cmp_ok($export->write_sample_assays($address, $test_file), '==', 96,
           "Number of records written to $test_file");

    ok(compare($test_file, $expected_file) == 0,
       "$test_file is identical to $expected_file");

    unlink $test_file;
  }
};
