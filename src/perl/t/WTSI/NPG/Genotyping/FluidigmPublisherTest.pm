
use utf8;

package WTSI::NPG::Genotyping::FluidigmPublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 7;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmPublisher') };

use WTSI::NPG::Genotyping::FluidigmPublisher;
use WTSI::NPG::Genotyping::FluidigmResultSet;

use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri);

use WTSI::NPG::iRODS qw(find_objects_by_meta);

my $data_path = './t/fluidigm_publisher';
my $fluidigm_directory = "$data_path/0123456789";

my $resultset;
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  $resultset = WTSI::NPG::Genotyping::FluidigmResultSet->new
    (directory => $fluidigm_directory);

  $irods_tmp_coll = "FluidigmPublisherTest.$pid";
  system("imkdir", $irods_tmp_coll) == 0
    or die "Failed to create iRODS temp collection $irods_tmp_coll\n";
};

sub teardown : Test(teardown) {
  undef $resultset;

  system("irm", "-r", "-f", $irods_tmp_coll) == 0
    or die "Failed to remove iRODS temp collection $irods_tmp_coll\n";
};

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::FluidigmPublisher');
};

sub constructor : Test(1) {
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  new_ok('WTSI::NPG::Genotyping::FluidigmPublisher',
         [creator_uri => $creator_uri,
          publisher_uri => $publisher_uri,
          publication_time => $publication_time,
          resultset => $resultset]);
};

sub publish : Test(2) {
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  my $publisher = WTSI::NPG::Genotyping::FluidigmPublisher->new
    (creator_uri => $creator_uri,
     publisher_uri => $publisher_uri,
     publication_time => $publication_time,
     resultset => $resultset);

  my @addresses_to_publish = qw(S01);
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', scalar @addresses_to_publish,
         "Number of chunks published");

  my @data_objects = find_objects_by_meta($irods_tmp_coll,
                                          [fluidigm_plate => '1381735059'],
                                          [fluidigm_well => 'S01']);
  cmp_ok(scalar @data_objects, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");
};

sub publish_overwrite : Test(2) {
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  my $publisher = WTSI::NPG::Genotyping::FluidigmPublisher->new
    (creator_uri => $creator_uri,
     publisher_uri => $publisher_uri,
     publication_time => $publication_time,
     resultset => $resultset);

  my @addresses_to_publish = qw(S01);
  # Publish
  $publisher->publish($irods_tmp_coll, @addresses_to_publish);
  # Publishing again should be a no-op
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', 1, "Number of chunks published");

  my @data_objects = find_objects_by_meta($irods_tmp_coll,
                                          [fluidigm_plate => '1381735059'],
                                          [fluidigm_well => 'S01']);
  cmp_ok(scalar @data_objects, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");
};

