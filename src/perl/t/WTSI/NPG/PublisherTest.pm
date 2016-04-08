use utf8;

package WTSI::NPG::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(WTSI::NPG::Test);
use Test::More tests => 10;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Publisher'); }

use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Publisher;

my $data_path = './t/publisher';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("PublisherTest.$pid");
  $irods->add_collection($irods_tmp_coll);
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Publisher');
}

sub publish_file : Test(8) {
  my $irods = WTSI::NPG::iRODS->new;
  my $publisher = WTSI::NPG::Publisher->new(irods => $irods);
  my $time = DateTime->from_epoch(epoch => 0);

  my $publish_dest = $irods_tmp_coll;
  my $sample_meta = [[a => "x.$pid"], [b => "y.$pid"]];

  my $lorem_file = "$data_path/lorem.txt";
  my $lorem_path = "$publish_dest/39/a4/aa/lorem.txt";

  is($publisher->publish_file($lorem_file, $sample_meta, $publish_dest, $time),
     $lorem_path, 'Published new file');

  my $lorem_obj = WTSI::NPG::iRODS::DataObject->new($irods, $lorem_path);

  my $uid = `whoami`;
  chomp($uid);
  my $publisher_uri = "ldap://ldap.internal.sanger.ac.uk/" .
    "ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid=$uid)";

  my $expected_meta =
    [{attribute => 'a',                 value => "x.$pid"},
     {attribute => 'b',                 value => "y.$pid"},
     {attribute => 'dcterms:created',   value => $time->iso8601},
     {attribute => 'dcterms:creator',   value => 'http://www.sanger.ac.uk'},
     {attribute => 'dcterms:publisher', value => $publisher_uri},
     {attribute => 'md5',
      value => '39a4aa291ca849d601e4e5b8ed627a04'},
     {attribute => 'type',              value => 'txt'}];

  my $meta = $lorem_obj->metadata;
  is_deeply($meta, $expected_meta, 'Primary metadata added')
    or diag explain $meta;

  my $updated_lorem_file = "$data_path/update/lorem.txt";
  my $updated_lorem_path = "$publish_dest/8e/b0/18/lorem.txt";

  is($publisher->publish_file($updated_lorem_file, $sample_meta,
                              $publish_dest, DateTime->now),
     $updated_lorem_path, 'Updated a published file');

  my $updated_lorem_obj =
    WTSI::NPG::iRODS::DataObject->new($irods, $updated_lorem_path);
  is($updated_lorem_obj->get_avu('md5')->{value},
     '8eb0180f3f882bb2e6d29998d1a0d323', 'Updated md5');
  ok($updated_lorem_obj->get_avu('dcterms:modified'),
     'Added modification time');
  is($updated_lorem_obj->calculate_checksum,
     '8eb0180f3f882bb2e6d29998d1a0d323', 'Updated file contents');
  ok($updated_lorem_obj->validate_checksum_metadata,
     'New md5 metadata matches file');

  my $lorem_obj_presence = $lorem_obj->is_present;
  ok(!$lorem_obj_presence, 'Update moved data object');

  # calling $lorem_obj->is_present raises warnings (because the object is
  # not present). These warnings are written to the test log. Calling
  # is_present *outside* the test assertion (as above) behaves as
  # expected.
  #
  # If the call is made *within* the test assertion, for example:
  # ok(!$lorem_obj->is_present, 'Update moved data object');
  # then the warnings are also printed to the terminal. The reason
  # for this is unclear, as the test log should be configured to direct
  # them to the logfile. For the time being, the is_present call has been
  # left outside the assertion to suppress unwanted output in the terminal
  # window.
}
