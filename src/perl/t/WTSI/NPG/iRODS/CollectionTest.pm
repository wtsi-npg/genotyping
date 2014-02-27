
use utf8;

package WTSI::NPG::iRODS::CollectionTest;

use strict;
use warnings;
use File::Spec;
use List::AllUtils qw(all any none);

use base qw(Test::Class);
use Test::More tests => 40;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::iRODS::Collection'); }

use WTSI::NPG::iRODS::Collection;

my $data_path = './t/irods_path_test';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods_tmp_coll = $irods->add_collection("CollectionTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $i = 0;
  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir";
      my $units = $value eq 'x' ? 'cm' : undef;

      $irods->add_collection_avu($test_coll, $attr, $value, $units);
    }
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::Collection');
}

sub constructor : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '.']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, './']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '/']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '/foo']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '/foo/bar']);
}

sub collection : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;

  my $path1 = WTSI::NPG::iRODS::Collection->new($irods, '.');
  ok($path1->has_collection, 'Has collection 1');
  is($path1->collection, '.');

  my $path2 = WTSI::NPG::iRODS::Collection->new($irods, '/');
  ok($path2->has_collection, 'Has collection 2');
  is($path2->collection, '/');

  my $path3 = WTSI::NPG::iRODS::Collection->new($irods, '/foo/');
  ok($path3->has_collection, 'Has collection 3');
  is($path3->collection, '/foo/');
}

sub is_present : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  ok($coll->is_present, 'Collection is present');

  ok(!WTSI::NPG::iRODS::Collection->new
     ($irods, "/no_such_object_collection")->is_present,
     'Collection is not present');
}

sub absolute : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;
  my $wc = $irods->working_collection;

  my $coll1 = WTSI::NPG::iRODS::Collection->new($irods, ".");
  is($coll1->absolute->str, $wc, 'Absolute collection from relative 1');

  my $coll2 = WTSI::NPG::iRODS::Collection->new($irods, "./");
  is($coll2->absolute->str, $wc, 'Absolute collection from relative 2');

  my $coll3 = WTSI::NPG::iRODS::Collection->new($irods, "/");
  is($coll3->absolute->str, '/', 'Absolute collection from relative 3');

  my $coll4 = WTSI::NPG::iRODS::Collection->new($irods, "foo");
  is($coll4->absolute->str, "$wc/foo", 'Absolute collection from relative 4');
}

sub metadata : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
  is_deeply($coll->metadata, $expected_meta,
            'Collection metadata loaded') or diag explain $coll->metadata;
}

sub get_avu : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my $avu = $coll->get_avu('a', 'x');
  is_deeply($avu, {attribute => 'a', value => 'x', units => 'cm'},
            'Matched one AVU 1');

  ok(!$coll->get_avu('does_not_exist', 'does_not_exist'),
     'Handles missing AVU');

  dies_ok { $coll_path->get_avu('a') }
    "Expected to fail getting ambiguous AVU";
}

sub add_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'a', value => 'z'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'b', value => 'z'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'},
                       {attribute => 'c', value => 'z'}];

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
  ok($coll->add_avu('a' => 'z'));
  ok($coll->add_avu('b' => 'z'));
  ok($coll->add_avu('c' => 'z'));

  my $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs added 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $coll->clear_metadata;

  $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs added 2') or diag explain $meta;
}

sub remove_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $expected_meta = [{attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'x', units => 'cm'}];

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
  ok($coll->remove_avu('a' => 'x', 'cm'));
  ok($coll->remove_avu('b' => 'y'));
  ok($coll->remove_avu('c' => 'y'));

  my $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs removed 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $coll->clear_metadata;

  $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs removed 2') or diag explain $meta;
}

sub str : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  is($coll->str, $coll_path, 'Collection string');
}

sub get_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my $perms = all { exists $_->{owner} &&
                    exists $_->{level} }
    $coll->get_permissions;
  ok($perms, 'Permissions obtained');
}

sub set_permissions : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll_path = "$irods_tmp_coll/irods_path_test/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $coll->get_permissions;
  ok($r0, 'No public read access');

  ok($coll->set_permissions('read', 'public'));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $coll->get_permissions;
  ok($r1, 'Added public read access');

  ok($coll->set_permissions(undef, 'public'));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $coll->get_permissions;
  ok($r2, 'Removed public read access');
}

1;
