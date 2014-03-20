use utf8;

package WTSI::NPG::Expression::ProfileAnnotationTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 7;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::ProfileAnnotation'); }

use WTSI::NPG::Expression::ControlProfileHint;
use WTSI::NPG::Expression::ProfileAnnotationHint;
use WTSI::NPG::Expression::ProfileHint;

my $data_path = './t/expression_analysis_publisher/data/analysis';

my $no_norm_file       = 'no_norm_Sample_Probe_Profile.txt';
my $cubic_norm_file    = 'cubic_norm_Sample_Probe_Profile.txt';
my $quantile_norm_file = 'quantile_norm_Sample_Probe_Profile.txt';
my $control_file       = 'Control_Probe_Profile.txt';
my $annotation_file    = 'profile_annotation.txt';

my $pid = $$;

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::ProfileAnnotation');
}

sub constructor : Test(1) {
  new_ok('WTSI::NPG::Expression::ProfileAnnotation',
         [file_name => "$data_path/$annotation_file"]);
}

sub guess : Test(4) {
  ok(WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$annotation_file")->guess);

  ok(!WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$no_norm_file")->guess);

  my $profile = WTSI::NPG::Expression::ProfileAnnotation->new
    ("$data_path/$annotation_file");

  # Test disambiguation when there are multiple hints. This is used
  # when there are an undefined mixture of files in a directory
  $profile->add_hint(WTSI::NPG::Expression::ControlProfileHint->new);
  $profile->add_hint(WTSI::NPG::Expression::ProfileHint->new);

  # These formats are ambiguous to a degree where voting is required
  ok($profile->guess);

  #$profile->voting_enabled(1);
  ok($profile->guess);
}
