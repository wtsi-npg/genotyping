package WTSI::NPG::Expression::ProfileAnnotationTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 10;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::ProfileAnnotation'); }

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

sub constructor : Test(3) {
  new_ok('WTSI::NPG::Expression::ProfileAnnotation',
         [file_name => "$data_path/$annotation_file"]);

  new_ok('WTSI::NPG::Expression::ProfileAnnotation',
         ["$data_path/$annotation_file"]);

  # Invalid format, but should be able to construct
  new_ok('WTSI::NPG::Expression::ProfileAnnotation',
         ["$data_path/$no_norm_file"]);
}

sub is_valid : Test(5) {
  ok(WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$annotation_file")->is_valid);

  ok(!WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$no_norm_file")->is_valid);

  ok(!WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$cubic_norm_file")->is_valid);

   ok(!WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$quantile_norm_file")->is_valid);

  ok(!WTSI::NPG::Expression::ProfileAnnotation->new
     ("$data_path/$control_file")->is_valid);
}

1;
