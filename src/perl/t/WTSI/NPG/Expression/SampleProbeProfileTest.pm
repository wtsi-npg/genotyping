use utf8;

package WTSI::NPG::Expression::SampleProbeProfileTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 16;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::SampleProbeProfile'); }

use WTSI::NPG::Expression::SampleProbeProfile;

my $data_path = './t/expression_analysis_publisher/data/analysis';

my $no_norm_file       = 'no_norm_Sample_Probe_Profile.txt';
my $cubic_norm_file    = 'cubic_norm_Sample_Probe_Profile.txt';
my $quantile_norm_file = 'quantile_norm_Sample_Probe_Profile.txt';
my $control_file       = 'Control_Probe_Profile.txt';
my $annotation_file    = 'profile_annotation.txt';

my $pid = $$;

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::SampleProbeProfile');
}

sub constructor : Test(4) {
  new_ok('WTSI::NPG::Expression::SampleProbeProfile',
         [file_name => "$data_path/$no_norm_file"]);

  new_ok('WTSI::NPG::Expression::SampleProbeProfile',
         ["$data_path/$no_norm_file"]);

  # Invalid format, but should be able to construct
  new_ok('WTSI::NPG::Expression::SampleProbeProfile',
         ["$data_path/$control_file"]);
}

sub is_valid : Test(5) {
  ok(WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$no_norm_file")->is_valid);

  ok(WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$cubic_norm_file")->is_valid);

  ok(WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$quantile_norm_file")->is_valid);

  ok(!WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$control_file")->is_valid);

  ok(!WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$annotation_file")->is_valid);
}

sub normalisation_method : Test(5) {
  my $x = WTSI::NPG::Expression::SampleProbeProfile->new
    ("$data_path/$no_norm_file");

  is(WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$no_norm_file")->normalisation_method, 'none',
     "normalisation method 1");

  is(WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$cubic_norm_file")->normalisation_method, 'cubic spline',
     "normalisation method 2");

  is(WTSI::NPG::Expression::SampleProbeProfile->new
     ("$data_path/$quantile_norm_file")->normalisation_method, 'quantile',
     "normalisation method 3");

  dies_ok { WTSI::NPG::Expression::SampleProbeProfile->new
      ("$data_path/$control_file")->normalisation_method }
    'Invalid file format 1';

  dies_ok { WTSI::NPG::Expression::SampleProbeProfile->new
      ("$data_path/$annotation_file")->normalisation_method }
    'Invalid file format 2';
}
