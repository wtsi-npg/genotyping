

use utf8;

{
  package WTSI::NPG::Database::WarehouseStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Database::Warehouse';

}

package WTSI::NPG::Expression::AnalysisPublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 3;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::AnalysisPublisher') };

use WTSI::NPG::Expression::ChipLoadingManifestV2;
use WTSI::NPG::Expression::AnalysisPublisher;
use WTSI::NPG::iRODS;

my $data_path = './t/expression_analysis_publisher';
my $manifest_path = "$data_path/manifest_v2.txt";
my $sample_data_path = "$data_path/samples";
my $analysis_data_path = "$data_path/analysis";

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll =
    $irods->add_collection("ExpressionAnalysisPublisherTest.$pid");

}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::AnalysisPublisher');
};

sub constructor : Test(1) {
  my $now = DateTime->now;
  my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
    (file_name => $manifest_path);

  my $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  new_ok('WTSI::NPG::Expression::AnalysisPublisher',
         [analysis_directory => $analysis_data_path,
          manifest           => $manifest,
          publication_time   => $now,
          sequencescape_db   => $ssdb]);
}

# sub resultsets : Test(1) {
#   my $now = DateTime->now;

#   my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
#     (file_name => $manifest_path);
#   my $ssdb = WTSI::NPG::Database::WarehouseStub->new
#     (name => 'sequencescape_warehouse',
#      inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

#   my $publisher = WTSI::NPG::Expression::AnalysisPublisher->new
#     (analysis_directory => $analysis_data_path,
#      manifest           => $manifest,
#      publication_time   => $now,
#      sequencescape_db   => $ssdb);

#   cmp_ok(scalar @{$publisher->resultsets}, '==', 21,
#          'Expected number of resultsets');
# }

# sub publish : Test(0) {
#   my $now = DateTime->now;

#   my $manifest =  WTSI::NPG::Expression::ChipLoadingManifestV2->new
#     (file_name => $manifest_path);
#   my $ssdb = WTSI::NPG::Database::WarehouseStub->new
#     (name => 'sequencescape_warehouse',
#      inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

#   my $publisher = WTSI::NPG::Expression::AnalysisPublisher->new
#     (manifest         => $manifest,
#      publication_time => $now,
#      sequencescape_db => $ssdb);


# }
