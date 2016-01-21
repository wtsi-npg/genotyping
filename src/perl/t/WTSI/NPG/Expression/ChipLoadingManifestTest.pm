use utf8;

package WTSI::NPG::Expression::ChipLoadingManifestTest;

use strict;
use warnings;
use DateTime;

use base qw(WTSI::NPG::Test);
use Test::More tests => 8;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::ChipLoadingManifestV1') };
BEGIN { use_ok('WTSI::NPG::Expression::ChipLoadingManifestV2') };

use WTSI::NPG::Expression::ChipLoadingManifestV1;
use WTSI::NPG::Expression::ChipLoadingManifestV2;

my $data_path = './t/expression_chip_loading_manifest';
my $manifest_v1 = "$data_path/manifest_v1.txt";
my $manifest_v2 = "$data_path/manifest_v2.txt";

sub require : Test(2) {
  require_ok('WTSI::NPG::Expression::ChipLoadingManifestV1');
  require_ok('WTSI::NPG::Expression::ChipLoadingManifestV2');
};

sub constructor : Test(2) {
  new_ok('WTSI::NPG::Expression::ChipLoadingManifestV1',
         [file_name => $manifest_v1]);

  new_ok('WTSI::NPG::Expression::ChipLoadingManifestV2',
         [file_name => $manifest_v2]);
}

sub samples : Test(2) {
  my $manifest1 = WTSI::NPG::Expression::ChipLoadingManifestV1->new
    (file_name => $manifest_v1);

  cmp_ok(scalar @{$manifest1->samples}, '==', 9);

  my $manifest2 = WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_v2);

  cmp_ok(scalar @{$manifest2->samples}, '==', 21);
}

1;
