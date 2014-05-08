
use utf8;

package WTSI::NPG::iRODS::DataObjectTest;

use strict;
use warnings;
use File::Spec;
use List::AllUtils qw(all any none);
use Log::Log4perl;

use base qw(Test::Class);
use Test::More tests => 54;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::iRODS::DataObject'); }

use WTSI::NPG::iRODS::DataObject;

my $data_path = './t/irods_path_test';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods_tmp_coll = $irods->add_collection("DataObjectTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $i = 0;
  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir";
      my $test_obj = File::Spec->join($test_coll, 'test_file.txt');
      my $units = $value eq 'x' ? 'cm' : undef;

      $irods->add_object_avu($test_obj, $attr, $value, $units);
    }
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::DataObject');
}

sub constructor : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;

  new_ok('WTSI::NPG::iRODS::DataObject', [irods       => $irods,
                                          collection  => '/foo',
                                          data_object => 'bar.txt']);

  new_ok('WTSI::NPG::iRODS::DataObject', [irods       => $irods,
                                          data_object => 'bar.txt']);

  new_ok('WTSI::NPG::iRODS::DataObject', [irods       => $irods,
                                          data_object => './bar.txt']);
}

sub data_object : Test(12) {
  my $irods = WTSI::NPG::iRODS->new;

  my $path1 = WTSI::NPG::iRODS::DataObject->new($irods, '/foo/bar.txt');
  ok($path1->has_collection, 'Has collection 1');
  ok($path1->has_data_object, 'Has data object 1');
  is($path1->collection, '/foo');
  is($path1->data_object, 'bar.txt');

  my $path2 = WTSI::NPG::iRODS::DataObject->new($irods, 'bar.txt');
  ok($path2->has_collection, 'Has collection 2');
  ok($path2->has_data_object, 'Has data object 2');
  is($path2->collection, '.');
  is($path2->data_object, 'bar.txt');

  my $path3 = WTSI::NPG::iRODS::DataObject->new($irods, './bar.txt');
  ok($path3->has_collection, 'Has collection 3');
  ok($path3->has_data_object, 'Has data object 3');
  is($path3->collection, '.');
  is($path3->data_object, 'bar.txt');
}

sub is_present : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  ok($obj->is_present, 'Object is present');

  ok(!WTSI::NPG::iRODS::DataObject->new
     ($irods, "no_such_object.txt")->is_present);
}

sub absolute : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;
  my $wc = $irods->working_collection;

  my $obj1 = WTSI::NPG::iRODS::DataObject->new($irods, "./foo.txt");
  is($obj1->absolute->str, "$wc/foo.txt", 'Absolute path from relative 1');

  my $obj2 = WTSI::NPG::iRODS::DataObject->new($irods, "foo.txt");
  is($obj2->absolute->str, "$wc/foo.txt", 'Absolute path from relative 2');

  my $obj3 = WTSI::NPG::iRODS::DataObject->new($irods, "/foo.txt");
  is($obj3->absolute->str, '/foo.txt', 'Absolute path from relative 3');
}

sub metadata : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  is_deeply($obj->metadata, $expected_meta,
            'DataObject metadata loaded') or diag explain $obj->metadata;
}

sub get_avu : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  my $avu = $obj->get_avu('a', 'x');
  is_deeply($avu, {attribute => 'a', value => 'x', units => 'cm'},
            'Matched one AVU 1');

  ok(!$obj->get_avu('does_not_exist', 'does_not_exist'), 'Handles missing AVU');

  dies_ok { $obj_path->get_avu('a') }
    "Expected to fail getting ambiguous AVU";
}

sub add_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'a', value => 'z'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'b', value => 'z'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'},
                       {attribute => 'c', value => 'z'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  ok($obj->add_avu('a' => 'z'));
  ok($obj->add_avu('b' => 'z'));
  ok($obj->add_avu('c' => 'z'));

  my $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs added 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs added 2') or diag explain $meta;
}

sub remove_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'x', units => 'cm'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  ok($obj->remove_avu('a' => 'x', 'cm'));
  ok($obj->remove_avu('b' => 'y'));
  ok($obj->remove_avu('c' => 'y'));

  my $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs removed 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs removed 2') or diag explain $meta;
}
sub supersede_avus : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'new_a'},
                       {attribute => 'b', value => 'new_b', units => 'km'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  ok($obj->supersede_avus('a' => 'new_a'));
  ok($obj->supersede_avus('b' => 'new_b', 'km'));

  my $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs superseded 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs superseded 2') or diag explain $meta;
}

sub str : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  is($obj->str, $obj_path, 'DataObject string');
}

sub get_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  my $perms = all { exists $_->{owner} &&
                    exists $_->{level} }
   $obj->get_permissions;
  ok($perms, 'Permissions obtained');
}

sub set_permissions : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $obj->get_permissions;
  ok($r0, 'No public read access');

  ok($obj->set_permissions('read', 'public'));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $obj->get_permissions;
  ok($r1, 'Added public read access');

  ok($obj->set_permissions(undef, 'public'));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $obj->get_permissions;
  ok($r2, 'Removed public read access');
}

sub get_groups : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  ok($irods->set_object_permissions('read', 'public', $obj_path));
  ok($irods->set_object_permissions('read', 'ss_0',   $obj_path));
  ok($irods->set_object_permissions('read', 'ss_10',  $obj_path));

  my $expected_all = ['ss_0', 'ss_10'];
  my @found_all  = $obj->get_groups;
  is_deeply(\@found_all, $expected_all, 'Expected all groups')
    or diag explain \@found_all;

  my $expected_read = ['ss_0', 'ss_10'];
  my @found_read = $obj->get_groups('read');
  is_deeply(\@found_read, $expected_read, 'Expected read groups')
    or diag explain \@found_read;

  my $expected_own = [];
  my @found_own  = $obj->get_groups('own');
  is_deeply(\@found_own, $expected_own, 'Expected own groups')
    or diag explain \@found_own;
}

1;
