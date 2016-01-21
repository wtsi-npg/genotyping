
use utf8;

package WTSI::NPG::Genotyping::SNPSetPublisherTest;

use strict;
use warnings;

use File::Compare;
use File::Temp qw(tempdir);

use base qw(WTSI::NPG::Test);
use File::Spec;
use Test::More tests => 8;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::SNPSetPublisher'); }

use WTSI::NPG::Genotyping::SNPSetPublisher;
use WTSI::NPG::iRODS;

my $data_path = './t/snpset';
my $data_file = 'qc.tsv';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SNPSetPublisherTest.$pid");

  $irods->put_collection($data_path, $irods_tmp_coll);
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::SNPSetPublisher');
}

sub constructor : Test(2) {
  my $publication_time = DateTime->now;
  my $reference_name1  = 'test_reference1';
  my $reference_name2  = 'test_reference2';
  my $snpset_name      = 'test_snpset';
  my $snpset_platform  = 'fluidigm';

  new_ok('WTSI::NPG::Genotyping::SNPSetPublisher',
        [file_name        => "$data_path/$data_file",
         publication_time => $publication_time,
         reference_names  => [$reference_name1, $reference_name2],
         snpset_name      => $snpset_name,
         snpset_platform  => $snpset_platform]);

  dies_ok {
    WTSI::NPG::Genotyping::SNPSetPublisher->new
        (file_name        => "$data_path/$data_file",
         publication_time => $publication_time,
         reference_names  => [$reference_name1, $reference_name2],
         snpset_name      => $snpset_name,
         snpset_platform  => 'unknown_platform');
  } 'Fails on unknown genotyping platform';
}

sub publish : Test(4) {
  my $file_name        = "$data_path/$data_file";
  my $publication_time = DateTime->now;

  my $snpset_name      = 'test_snpset';
  my $snpset_platform  = 'fluidigm';
  my $reference_name1  = 'test_reference1';
  my $reference_name2  = 'test_reference2';
  my $publish_dest = $irods_tmp_coll;

  my $publisher = WTSI::NPG::Genotyping::SNPSetPublisher->new
    (file_name        => $file_name,
     publication_time => $publication_time,
     reference_names  => [$reference_name1, $reference_name2],
     snpset_name      => $snpset_name,
     snpset_platform  => $snpset_platform);

  is($publisher->publish($publish_dest), "$publish_dest/$data_file",
     'Published new file');

  my $uid = `whoami`;
  chomp($uid);
  my $publisher_uri = "ldap://ldap.internal.sanger.ac.uk/" .
    "ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid=$uid)";

  my $expected_meta =
    [{attribute => 'dcterms:created',   value => $publication_time->iso8601},
     {attribute => 'dcterms:creator',   value => 'http://www.sanger.ac.uk'},
     {attribute => 'dcterms:publisher', value => $publisher_uri},
     {attribute => $snpset_platform . "_plex",
      value => $snpset_name},
     {attribute => 'md5',
      value => '66935a3f9084e3c695e3f39a65882968'},
     {attribute => 'reference_name',    value => $reference_name1},
     {attribute => 'reference_name',    value => $reference_name2},
     {attribute => 'type',              value => 'tsv'}];

  my $irods = WTSI::NPG::iRODS->new;
  my $snpset_obj =
    WTSI::NPG::iRODS::DataObject->new($irods, "$publish_dest/$data_file");

  my $meta = $snpset_obj->metadata;
  is_deeply($meta, $expected_meta, 'SNPSet metadata added')
    or diag explain $meta;

  # Find by first reference
  my @found1 = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [$snpset_platform . "_plex" => $snpset_name],
     ['reference_name'           => $reference_name1]);
  cmp_ok(scalar @found1, '==', 1, "Number SNPSets found");

  # Find by second reference
  my @found2 = $irods->find_objects_by_meta
    ($irods_tmp_coll,
     [$snpset_platform . "_plex" => $snpset_name],
     ['reference_name'           => $reference_name2]);
  cmp_ok(scalar @found2, '==', 1, "Number SNPSets found");
}

1;
