
use utf8;

package WTSI::NPG::iRODS::PathTest;

use strict;
use warnings;
use File::Spec;

use base qw(Test::Class);
use Test::More tests => 39;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS qw(add_collection
                        add_collection_meta
                        add_object_meta
                        put_collection
                        remove_collection);

BEGIN { use_ok('WTSI::NPG::iRODS::Path'); }

use WTSI::NPG::iRODS::Path;

my $data_path = './t/irods_path_test/';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  $irods_tmp_coll = add_collection("PathTest.$pid");
  put_collection($data_path, $irods_tmp_coll);

  my $i = 0;
  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir/";
      my $test_obj = File::Spec->join($test_coll, 'test_file.txt');
      add_collection_meta($test_coll, $attr, $value);
      add_object_meta($test_obj, $attr, $value);
    }
  }
};

sub teardown : Test(teardown) {
  remove_collection($irods_tmp_coll);
};

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::Path');
};

sub constructor : Test(8) {
  new_ok('WTSI::NPG::iRODS::Path', ['.']);

  new_ok('WTSI::NPG::iRODS::Path', ['./']);

  new_ok('WTSI::NPG::iRODS::Path', ['/']);

  new_ok('WTSI::NPG::iRODS::Path', ['/foo/']);

  new_ok('WTSI::NPG::iRODS::Path', ['/foo/bar/']);

  new_ok('WTSI::NPG::iRODS::Path', ['/foo/bar.txt']);

  new_ok('WTSI::NPG::iRODS::Path', ['bar.txt']);

  new_ok('WTSI::NPG::iRODS::Path', ['./bar.txt']);
};

sub collection : Test(9) {
  my $path1 = WTSI::NPG::iRODS::Path->new('.');
  ok($path1->has_collection, 'Has collection 1');
  ok(!$path1->has_data_object, 'Has no data object 1');
  is($path1->collection, '.');

  my $path2 = WTSI::NPG::iRODS::Path->new('/');
  ok($path2->has_collection, 'Has collection 2');
  ok(!$path2->has_data_object, 'Has no data object 2');
  is($path2->collection, '/');

  my $path3 = WTSI::NPG::iRODS::Path->new('/foo/');
  ok($path3->has_collection, 'Has collection 3');
  ok(!$path3->has_data_object, 'Has no data object 3');
  is($path3->collection, '/foo/');
};

sub data_object : Test(12) {
  my $path1 = WTSI::NPG::iRODS::Path->new('/foo/bar.txt');
  ok($path1->has_collection, 'Has collection 1');
  ok($path1->has_data_object, 'Has data object 1');
  is($path1->collection, '/foo/');
  is($path1->data_object, 'bar.txt');

  my $path2 = WTSI::NPG::iRODS::Path->new('bar.txt');
  ok($path2->has_collection, 'Has collection 2');
  ok($path2->has_data_object, 'Has data object 2');
  is($path2->collection, '');
  is($path2->data_object, 'bar.txt');

  my $path3 = WTSI::NPG::iRODS::Path->new('./bar.txt');
  ok($path3->has_collection, 'Has collection 3');
  ok($path3->has_data_object, 'Has data object 3');
  is($path3->collection, './');
  is($path3->data_object, 'bar.txt');
};

sub metadata : Test(2) {
  my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir/";
  my $test_obj = "$test_coll/test_file.txt";

  my $expected_meta = [['a' => 'x', ''],
                       ['a' => 'y', ''],
                       ['b' => 'x', ''],
                       ['b' => 'y', ''],
                       ['c' => 'x', ''],
                       ['c' => 'y', '']];

  my $coll_path = WTSI::NPG::iRODS::Path->new($test_coll);
  is_deeply($coll_path->metadata, $expected_meta,
            'Coll metadata loaded') or diag explain $coll_path->metadata;

  my $obj_path = WTSI::NPG::iRODS::Path->new($test_obj);
  is_deeply($obj_path->metadata, $expected_meta,
            'Obj metadata loaded') or diag explain $obj_path->metadata;
}

sub get_avu : Test(4) {
  my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir/";

  my $coll_path = WTSI::NPG::iRODS::Path->new($test_coll);

  my @avu1 = $coll_path->get_avu('a', 'x');
  is_deeply(\@avu1, ['a', 'x', ''], 'Matched one AVU 1');

  my @avu2 = $coll_path->get_avu('a', 'x', '');
  is_deeply(\@avu2, ['a', 'x', ''], 'Matched one AVU 2');

  my @avu3 = $coll_path->get_avu('does_not_exist', 'does_not_exist');
  is_deeply(\@avu3, [], 'Handles missing AVU');

  dies_ok { $coll_path->get_avu('a') }
    "Expected to fail getting ambiguous AVU";
}

sub add_avu : Test(1) {
  my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir/";

  my $expected_meta = [['a' => 'x', ''],
                       ['a' => 'y', ''],
                       ['a' => 'z', ''],
                       ['b' => 'x', ''],
                       ['b' => 'y', ''],
                       ['b' => 'z', ''],
                       ['c' => 'x', ''],
                       ['c' => 'y', ''],
                       ['c' => 'z', '']];

  my $coll_path = WTSI::NPG::iRODS::Path->new($test_coll);
  $coll_path->add_avu('a' => 'z');
  $coll_path->add_avu('b' => 'z');
  $coll_path->add_avu('c' => 'z');

  my $meta = $coll_path->metadata;
  is_deeply($meta, $expected_meta,
            'Coll metadata AVUs added') or diag explain $meta;
}

sub remove_avu : Test(1) {
  my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir/";

  my $expected_meta = [['a' => 'x', ''],
                       ['b' => 'x', ''],
                       ['c' => 'x', '']];

  my $coll_path = WTSI::NPG::iRODS::Path->new($test_coll);
  $coll_path->remove_avu('a' => 'y');
  $coll_path->remove_avu('b' => 'y');
  $coll_path->remove_avu('c' => 'y');

  my $meta = $coll_path->metadata;
  is_deeply($meta, $expected_meta,
            'coll metadata AVUs removed') or diag explain $meta;
}
