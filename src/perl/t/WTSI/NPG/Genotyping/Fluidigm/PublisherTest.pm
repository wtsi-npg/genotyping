
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 11;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher') };

use WTSI::NPG::Genotyping::Fluidigm::Publisher;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;
use WTSI::NPG::iRODS;

my $data_path = './t/fluidigm_publisher';
my $fluidigm_directory = "$data_path/0123456789";
my $snpset_file = 'qc.csv';

my $resultset;
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = "FluidigmPublisherTest.$pid";
  $irods->add_collection($irods_tmp_coll);
  $irods->add_object("$data_path/$snpset_file", "$irods_tmp_coll/$snpset_file");

  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file" )->absolute;
  $snpset_obj->add_avu('fluidigm_plex', 'qc');
  $snpset_obj->add_avu('reference_name', '1000Genomes');

  $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
    (directory => $fluidigm_directory);
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);

  undef $resultset;
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher');
}

sub constructor : Test(1) {
  my $publication_time = DateTime->now;

  new_ok('WTSI::NPG::Genotyping::Fluidigm::Publisher',
         [publication_time => $publication_time,
          resultset        => $resultset]);
}

sub publish : Test(4) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset);

  my @addresses_to_publish = qw(S01);
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', scalar @addresses_to_publish,
         "Number of chunks published");

  my $irods = WTSI::NPG::iRODS->new;
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
                                 [fluidigm_well  => 'S01'],
                                 [fluidigm_plex  => 'qc']);
  cmp_ok(scalar @chunked_data, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");

  @chunked_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate     => '1381735059'],
                                 [fluidigm_well      => 'S01'],
                                 [fluidigm_plex      => 'qc'],
                                 ['dcterms:audience' => "$audience%", 'like']);
  cmp_ok(scalar @chunked_data, '==', 0,
         "Number of chunks published with dcterms:audience metadata");
}

sub publish_overwrite : Test(2) {
  my $publication_time = DateTime->now;

  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset);

  my @addresses_to_publish = qw(S01);
  # Publish
  $publisher->publish($irods_tmp_coll, @addresses_to_publish);
  # Publishing again should be a no-op
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', 1, "Number of chunks published");

  my $irods = WTSI::NPG::iRODS->new;
  my @data_objects =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [fluidigm_plate => '1381735059'],
                                 [fluidigm_well  => 'S01'],
                                 [fluidigm_plex  => 'qc']);
  cmp_ok(scalar @data_objects, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");
}

sub publish_ambiguous_snpset : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  # Add a new copy of the SNP set containing the same SNPS, but having
  # a distinct name
  $irods->add_object("$data_path/$snpset_file",
                     "$irods_tmp_coll/$snpset_file.2");
  my $snpset_copy = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file.2")->absolute;
  $snpset_copy->add_avu('fluidigm_plex', 'qc2');
  $snpset_copy->add_avu('reference_name', '1000Genomes');

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset);

  my @addresses_to_publish = qw(S01);
  ok(!$publisher->publish($irods_tmp_coll, @addresses_to_publish),
     'Failed to publish with ambiguous SNP set data');
}

sub publish_ambiguous_metadata : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  # Add a new fluidigm_plex name to the existing SNP set
  my $snpset = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file")->absolute;
  $snpset->add_avu('fluidigm_plex', 'qc2');

  my $publication_time = DateTime->now;
  my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
    (publication_time => $publication_time,
     resultset        => $resultset);

  my @addresses_to_publish = qw(S01);
  ok(!$publisher->publish($irods_tmp_coll, @addresses_to_publish),
     'Failed to publish with abmiguous SNP set metadata');
}
