
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 9;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher') };

use WTSI::NPG::Genotyping::Fluidigm::Publisher;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;

use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri);

use WTSI::NPG::iRODS2;

my $data_path = './t/fluidigm_publisher';
my $fluidigm_directory = "$data_path/0123456789";

my $resultset;
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS2->new;
  $irods_tmp_coll = "FluidigmPublisherTest.$pid";
  $irods->add_collection($irods_tmp_coll);

  $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
    (directory => $fluidigm_directory);
};

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS2->new;
  $irods->remove_collection($irods_tmp_coll);

  undef $resultset;
};

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher');
};

sub constructor : Test(1) {
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  new_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher',
         [creator_uri      => $creator_uri,
          publisher_uri    => $publisher_uri,
          publication_time => $publication_time,
          resultset        => $resultset]);
};

sub publish : Test(4) {
  my $irods = WTSI::NPG::iRODS2->new;

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (creator_uri      => $creator_uri,
     publisher_uri    => $publisher_uri,
     publication_time => $publication_time,
     resultset        => $resultset);

  my @addresses_to_publish = qw(S01);
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', scalar @addresses_to_publish,
         "Number of chunks published");

  my $audience = 'http://psd-production.internal.sanger.ac.uk';
  my @aggregate_data =
     $irods->find_objects_by_meta($irods_tmp_coll,
                                  [fluidigm_plate     => '1381735059'],
                                  ['dcterms:audience' => "$audience%", 'like']);
  cmp_ok(scalar @aggregate_data, '==', 1,
         "Number of aggregate data published with dcterms:audience metadata");

  my @chunked_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate => '1381735059'],
                                 [fluidigm_well  => 'S01']);
  cmp_ok(scalar @chunked_data, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");

  @chunked_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate => '1381735059'],
                                 [fluidigm_well  => 'S01'],
                                 ['dcterms:audience' => "$audience%", 'like']);
  cmp_ok(scalar @chunked_data, '==', 0,
         "Number of chunks published with dcterms:audience metadata");
};

sub publish_overwrite : Test(2) {
  my $irods = WTSI::NPG::iRODS2->new;

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (creator_uri      => $creator_uri,
     publisher_uri    => $publisher_uri,
     publication_time => $publication_time,
     resultset        => $resultset);

  my @addresses_to_publish = qw(S01);
  # Publish
  $publisher->publish($irods_tmp_coll, @addresses_to_publish);
  # Publishing again should be a no-op
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', 1, "Number of chunks published");

  my @data_objects =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate => '1381735059'],
                                 [fluidigm_well  => 'S01']);
  cmp_ok(scalar @data_objects, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");
};

