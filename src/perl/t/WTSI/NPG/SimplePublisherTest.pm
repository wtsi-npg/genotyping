use utf8;

package WTSI::NPG::SimplePublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 7;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::SimplePublisher'); }

use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::SimplePublisher;

my $data_path = './t/simple_publisher';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SimplePublisherTest.$pid");
  $irods->add_collection($irods_tmp_coll);
};

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
};

sub require : Test(1) {
  require_ok('WTSI::NPG::SimplePublisher');
};

sub publish_file : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $publisher = WTSI::NPG::SimplePublisher->new(irods => $irods);
  my $time = DateTime->from_epoch(epoch => 0);

  my $lorem_file = "$data_path/lorem.txt";
  my $sample_meta = [[a => "x.$pid"], [b => "y.$pid"]];
  my $publish_dest = $irods_tmp_coll;

  is($publisher->publish_file($lorem_file, $sample_meta, $publish_dest, $time),
     "$publish_dest/lorem.txt", 'Published new file');

  my $lorem_obj = WTSI::NPG::iRODS::DataObject->new($irods,
                                                    "$publish_dest/lorem.txt");

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

  my $update_lorem_file = "$data_path/update/lorem.txt";
  is($publisher->publish_file($update_lorem_file, $sample_meta,
                              $publish_dest, DateTime->now),
     "$publish_dest/lorem.txt", 'Updated a published file');

  $lorem_obj = WTSI::NPG::iRODS::DataObject->new($irods,
                                                 "$publish_dest/lorem.txt");
  is($lorem_obj->get_avu('md5')->{value}, '8eb0180f3f882bb2e6d29998d1a0d323',
     'Updated md5');
  ok($lorem_obj->get_avu('dcterms:modified'), 'Added modification time');
}

