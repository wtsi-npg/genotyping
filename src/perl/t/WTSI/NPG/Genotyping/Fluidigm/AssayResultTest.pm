
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResultTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use File::Spec;
use Test::More tests => 407;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResult'); }

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResult;

my $data_path = './t/fluidigm_assay_data_object/1381735059';
my $data_file = 'S01_1381735059.csv';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("FluidigmAssayResultTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/1381735059/$data_file";

  $irods->add_object_avu($irods_path, 'fluidigm_plate', '1381735059');
  $irods->add_object_avu($irods_path, 'fluidigm_well', 'S01');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResult');
}

sub constructor : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResult',
         [assay          => 'S01-A01',
          snp_assayed    => 'rs0123456',
          x_allele       => 'G',
          y_allele       => 'T',
          sample_name    => 'ABC0123456789',
          type           => 'Unknown',
          auto           => 'No Call',
          confidence     => 0.1,
          final          => 'XY',
          converted_call => 'G:T',
          x_intensity    => 0.1,
          y_intensity    => 0.1,
          str            => join("\t", 'S01-A0','rs0123456', 'G', 'T',
                                 'ABC0123456789', 'Unknown', 'No Call',
                                 0.1, 'XY', 'G:T', 0.1, 0.1)
         ]);
}

sub is_empty : Test(2) {
  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_empty, 'Is not empty');

  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A0',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => '[ Empty ]',
      type           => 'Unknown',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_empty, 'Is empty');
}

sub is_control : Test(4) {
  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_control, 'Is not control');

  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => '[ Empty ]',
      type           => 'Unknown',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_control, 'Is control 1');

  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'NTC',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_control, 'Is control 2');

  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A0',
      snp_assayed    => '',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_control, 'Is control 3');
}

sub is_call : Test(5) {
  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_call, 'Is call 1');

  # This is the conclusion from looking at counts of calls per SNP in
  # the Fluidigm PDF summary report i.e. 'No Call' in the auto column
  # does not necessarily mean a no call.
  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'No Call',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_call, 'Is call 2');

  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'No Call',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_call, 'Is not call 1');

  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'No Call',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_call, 'Is not call 2');

  # For 'Invalid' calls, is_call() should be false
  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'Invalid',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_call, 'Is not call 3');
}

sub is_template : Test(5) {

    # AssayResult is a template assay iff not empty and not control
    ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_template_assay, 'Is template assay 1');

    # Empty AssayResult is not a template assay
    ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => '[ Empty ]',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_template_assay, 'Is template assay 2');

    # Control AssayResult is not a template assay
    ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => '',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'NTC',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_template_assay, 'Is template assay 3');

    # AssayResult is a template assay *and* a call
    ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_template_call, 'Is template call 1');

    # AssayResult is a template assay *and not* a call
    ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'No Call',
      converted_call => 'No Call',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_template_call, 'Is template call 2');

}


sub is_valid : Test(4) {
  # Evaluate whether a result is valid. See is_call. 'No Call' and 'invalid'
  # are distinct and represent different experimental outcomes.

  ok(WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_valid, 'Is valid 1');

  # a 'no call' may still be valid
  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'XY',
      converted_call => 'No Call',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_call, 'Is valid 2');

  # invalid calls
  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'Invalid',
      converted_call => 'G:T',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_valid, 'Is not valid 1');

  # invalid calls
  ok(!WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
     (assay          => 'S01-A01',
      snp_assayed    => 'rs0123456',
      x_allele       => 'G',
      y_allele       => 'T',
      sample_name    => 'ABC0123456789',
      type           => 'Unknown',
      auto           => 'XY',
      confidence     => 0.1,
      final          => 'Invalid',
      converted_call => 'Invalid',
      x_intensity    => 0.1,
      y_intensity    => 0.1,
      str            => '')->is_valid, 'Is not valid 2');
}

sub sample_address : Test(192) {
  foreach my $s (1 .. 96) {
    foreach my $assay_part ('01', '96') {
      my $sample_part = sprintf("%02d", $s);

      my $result = WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
        (assay          => $sample_part . q{-} . $assay_part,
         snp_assayed    => 'rs0123456',
         x_allele       => 'G',
         y_allele       => 'T',
         sample_name    => 'ABC0123456789',
         type           => 'Unknown',
         auto           => 'XY',
         confidence     => 0.1,
         final          => 'Invalid',
         converted_call => 'Invalid',
         x_intensity    => 0.1,
         y_intensity    => 0.1,
         str            => '');

      is($result->sample_address, $sample_part, "Sample address $s");
    }
  }
}

sub assay_address : Test(192) {
  foreach my $sample_part ('01', '96') {
    foreach my $a (1..96) {
      my $assay_part = sprintf("%02d", $a);

      my $result = WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
        (assay          => $sample_part . q{-} . $assay_part,
         snp_assayed    => 'rs0123456',
         x_allele       => 'G',
         y_allele       => 'T',
         sample_name    => 'ABC0123456789',
         type           => 'Unknown',
         auto           => 'XY',
         confidence     => 0.1,
         final          => 'Invalid',
         converted_call => 'Invalid',
         x_intensity    => 0.1,
         y_intensity    => 0.1,
         str            => '');

      is($result->assay_address, $assay_part, "Assay position $a");
    }
  }
}

1;
