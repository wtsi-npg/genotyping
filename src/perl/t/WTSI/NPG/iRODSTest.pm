
use utf8;

package WTSI::NPG::iRODSTest;

use strict;
use warnings;
use English;
use File::Spec;
use List::AllUtils qw(all any none);
use Unicode::Collate;

use base qw(Test::Class);
use Test::More tests => 147;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::iRODS'); }

use WTSI::NPG::iRODS;

my $pid = $PID;
my $cwc = WTSI::NPG::iRODS->new->working_collection;

my $data_path = './t/irods';
my $irods_tmp_coll;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods_tmp_coll = $irods->add_collection("iRODSTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $i = 0;
  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $test_coll = "$irods_tmp_coll/irods";
      my $test_obj = File::Spec->join($test_coll, 'lorem.txt');
      my $units = $value eq 'x' ? 'cm' : undef;

      $irods->add_collection_avu($test_coll, $attr, $value, $units);
      $irods->add_object_avu($test_obj, $attr, $value, $units);
    }
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;

  $irods->working_collection($cwc);
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS');
}

sub find_zone_name : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;

  like($irods->find_zone_name('/Sanger1'), qr{^Sanger1});
  is($irods->find_zone_name('/no_such_zone'), 'no_such_zone');
  ok($irods->find_zone_name('relative'),
     'Falls back to current zone for relative paths');
}

sub working_collection : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  like($irods->working_collection, qr{^/}, 'Found a working collection');

  isnt($irods->working_collection, '/');
  ok($irods->working_collection('/'), 'Set the working collection');
  is($irods->working_collection, '/', 'Working collection set');
}

sub list_groups : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  ok(grep { /^rodsadmin$/ } $irods->list_groups, 'Listed the rodsadmin group');
}

sub group_exists : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  ok($irods->group_exists('rodsadmin'), 'The rodsadmin group exists');
  ok(!$irods->group_exists('no_such_group'), 'An absent group does not exist');
}

sub set_group_access : Test(7) {
  my $irods = WTSI::NPG::iRODS->new;
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  dies_ok { $irods->set_group_access('no_such_permission', 'public',
                                     $lorem_object) }
    'Expected to fail setting access with an invalid permission argument';

  dies_ok { $irods->set_group_access('read', 'no_such_group_exists',
                                     $lorem_object) }
    'Expected to fail setting access for non-existent group';

  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r0, 'No public read access');

  ok($irods->set_group_access('read', 'public', $lorem_object));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r1, 'Added public read access');

  ok($irods->set_group_access(undef, 'public', $lorem_object));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r2, 'Removed public read access');
}

sub get_object_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  my $perms = all { exists $_->{owner} &&
                    exists $_->{level} }
    $irods->get_object_permissions($lorem_object);
  ok($perms, 'Permissions obtained');
}

sub set_object_permissions : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  dies_ok { $irods->set_object_permissions('no_such_permission', 'public',
                                           $lorem_object) }
    'Expected to fail setting access with an invalid permission argument';

  dies_ok { $irods->set_object_permissions('read', 'no_such_group_exists',
                                           $lorem_object) }
    'Expected to fail setting access for non-existent group';

  ok($irods->set_object_permissions('read', 'public', $lorem_object));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r1, 'Added public read access');

  ok($irods->set_object_permissions(undef, 'public', $lorem_object));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r2, 'Removed public read access');
}

sub get_object_groups : Test(6) {
   my $irods = WTSI::NPG::iRODS->new;
   my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

   ok($irods->set_object_permissions('read', 'public', $lorem_object));
   ok($irods->set_object_permissions('read', 'ss_0',   $lorem_object));
   ok($irods->set_object_permissions('read', 'ss_10',  $lorem_object));

   my $expected_all = ['ss_0', 'ss_10'];
   my @found_all  = $irods->get_object_groups($lorem_object);
   is_deeply(\@found_all, $expected_all, 'Expected all groups')
     or diag explain \@found_all;

   my $expected_read = ['ss_0', 'ss_10'];
   my @found_read = $irods->get_object_groups($lorem_object, 'read');
   is_deeply(\@found_read, $expected_read, 'Expected read groups')
     or diag explain \@found_read;

   my $expected_own = [];
   my @found_own  = $irods->get_object_groups($lorem_object, 'own');
   is_deeply(\@found_own, $expected_own, 'Expected own groups')
     or diag explain \@found_own;
}

sub get_collection_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll = "$irods_tmp_coll/irods";

  my $perms = all { exists $_->{owner} &&
                    exists $_->{level} }
    $irods->get_collection_permissions($coll);
  ok($perms, 'Permissions obtained');
}

sub set_collection_permissions : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll = "$irods_tmp_coll/irods";

  dies_ok { $irods->set_collection_permissions('no_such_permission', 'public',
                                               $coll) }
    'Expected to fail setting access with an invalid permission argument';

  dies_ok { $irods->set_collection_permissions('read', 'no_such_group_exists',
                                               $coll) }
    'Expected to fail setting access for non-existent group';

  ok($irods->set_collection_permissions('read', 'public', $coll));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_collection_permissions($coll);
  ok($r1, 'Added public read access');

  ok($irods->set_collection_permissions(undef, 'public', $coll));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_collection_permissions($coll);
  ok($r2, 'Removed public read access');
}

sub get_collection_groups : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;
  my $coll = "$irods_tmp_coll/irods";

  ok($irods->set_collection_permissions('read', 'public', $coll));
  ok($irods->set_collection_permissions('read', 'ss_0',   $coll));
  ok($irods->set_collection_permissions('read', 'ss_10',  $coll));

  my $expected_all = ['ss_0', 'ss_10'];
  my @found_all  = $irods->get_collection_groups($coll);
  is_deeply(\@found_all, $expected_all, 'Expected all groups')
    or diag explain \@found_all;

  my $expected_read = ['ss_0', 'ss_10'];
  my @found_read = $irods->get_collection_groups($coll, 'read');
  is_deeply(\@found_read, $expected_read, 'Expected read groups')
    or diag explain \@found_read;

  my $expected_own = [];
  my @found_own  = $irods->get_collection_groups($coll, 'own');
  is_deeply(\@found_own, $expected_own, 'Expected own groups')
    or diag explain \@found_own;
}

sub list_collection : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my ($objs, $colls) = $irods->list_collection("$irods_tmp_coll/irods");

  is_deeply($objs, ["$irods_tmp_coll/irods/lorem.txt",
                    "$irods_tmp_coll/irods/test.txt",
                    "$irods_tmp_coll/irods/utf-8.txt"]) or diag explain $objs;

  is_deeply($colls, ["$irods_tmp_coll/irods",
                     "$irods_tmp_coll/irods/collect_files",
                     "$irods_tmp_coll/irods/md5sum",
                     "$irods_tmp_coll/irods/test"]) or diag explain $colls;

   ok(!$irods->list_collection('no_collection_exists'),
      'Failed to list a non-existent collection');

  my ($objs_deep, $colls_deep) =
    $irods->list_collection("$irods_tmp_coll/irods", 'RECURSE');

  is_deeply($objs_deep, ["$irods_tmp_coll/irods/lorem.txt",
                         "$irods_tmp_coll/irods/test.txt",
                         "$irods_tmp_coll/irods/utf-8.txt",
                         "$irods_tmp_coll/irods/collect_files/a/10.txt",
                         "$irods_tmp_coll/irods/collect_files/a/x/1.txt",
                         "$irods_tmp_coll/irods/collect_files/b/20.txt",
                         "$irods_tmp_coll/irods/collect_files/b/y/2.txt",
                         "$irods_tmp_coll/irods/collect_files/c/30.txt",
                         "$irods_tmp_coll/irods/collect_files/c/z/3.txt",
                         "$irods_tmp_coll/irods/md5sum/lorem.txt",
                         "$irods_tmp_coll/irods/test/file1.txt",
                         "$irods_tmp_coll/irods/test/file2.txt",
                         "$irods_tmp_coll/irods/test/dir1/file3.txt",
                         "$irods_tmp_coll/irods/test/dir2/file4.txt"])
    or diag explain $objs_deep;

  is_deeply($colls_deep, ["$irods_tmp_coll/irods",
                          "$irods_tmp_coll/irods/collect_files",
                          "$irods_tmp_coll/irods/collect_files/a",
                          "$irods_tmp_coll/irods/collect_files/a/x",
                          "$irods_tmp_coll/irods/collect_files/b",
                          "$irods_tmp_coll/irods/collect_files/b/y",
                          "$irods_tmp_coll/irods/collect_files/c",
                          "$irods_tmp_coll/irods/collect_files/c/z",
                          "$irods_tmp_coll/irods/md5sum",
                          "$irods_tmp_coll/irods/test",
                          "$irods_tmp_coll/irods/test/dir1",
                          "$irods_tmp_coll/irods/test/dir2"])
    or diag explain $colls_deep;
}

sub add_collection : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  # Deliberate spaces in names
  my $coll = "$irods_tmp_coll/add_ _collection";
  is($irods->add_collection($coll), $coll, 'Created a collection');
  ok($irods->list_collection($coll), 'Listed a new collection');
}

sub put_collection : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $dir = File::Spec->catdir($data_path, 'test');
  my $target = "$irods_tmp_coll/put_collection";
  $irods->add_collection($target);

  is($irods->put_collection($dir, $target), "$target/test",
     'Put a new collection');

  my @contents = $irods->list_collection("$target/test");

  is_deeply(\@contents,
            [["$irods_tmp_coll/put_collection/test/file1.txt",
              "$irods_tmp_coll/put_collection/test/file2.txt"],
             ["$irods_tmp_coll/put_collection/test",
              "$irods_tmp_coll/put_collection/test/dir1",
              "$irods_tmp_coll/put_collection/test/dir2"]])
    or diag explain \@contents;
}

sub move_collection : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;

  my $coll_to_move = "$irods_tmp_coll/irods";
  my $coll_moved = "$irods_tmp_coll/irods_moved";

  is($irods->move_collection($coll_to_move, $coll_moved), $coll_moved,
     'Moved a collection');

  ok(!$irods->list_collection($coll_to_move), 'Collection was moved 1');
  ok($irods->list_collection($coll_moved), 'Collection was moved 2');

  dies_ok { $irods->move_collection($coll_to_move, undef) }
    'Failed to move a collection to an undefined place';
  dies_ok { $irods->move_collection(undef, $coll_moved) }
    'Failed to move an undefined collection';
}

sub remove_collection : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $coll = "$irods_tmp_coll/irods";
  is($irods->remove_collection($coll), $coll, 'Removed a collection');
  ok(!$irods->list_collection($coll), 'Collection was removed');

  dies_ok { $irods->remove_collection('/no_such_collection') }
    'Failed to remove a non-existent collection';
  dies_ok { $irods->remove_collection }
    'Failed to remove an undefined collection';
}

sub get_collection_meta : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $coll = "$irods_tmp_coll/irods";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_collection_meta($coll);

  is_deeply(\@observed_meta, $expected_meta,
            'Collection metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->get_collection_meta('/no_such_collection',
                                        'attr', 'value') }
    'Failed to get metadata from a non-existent collection';
}

sub add_collection_avu : Test(11) {
  my $irods = WTSI::NPG::iRODS->new;

  my $num_attrs = 8;
  my %meta = map { 'cattr' . $_ => 'cval' . $_ } 0 .. $num_attrs;

  my $test_coll = $irods_tmp_coll;
  foreach my $attr (keys %meta) {
    is($irods->add_collection_avu($test_coll, $attr, $meta{$attr}),
       $test_coll);
  }

  my $expected_meta = [{attribute => 'cattr0', value => 'cval0'},
                       {attribute => 'cattr1', value => 'cval1'},
                       {attribute => 'cattr2', value => 'cval2'},
                       {attribute => 'cattr3', value => 'cval3'},
                       {attribute => 'cattr4', value => 'cval4'},
                       {attribute => 'cattr5', value => 'cval5'},
                       {attribute => 'cattr6', value => 'cval6'},
                       {attribute => 'cattr7', value => 'cval7'},
                       {attribute => 'cattr8', value => 'cval8'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_collection_meta($test_coll);

  is_deeply(\@observed_meta, $expected_meta,
            'Collection metadata added') or diag explain \@observed_meta;

  dies_ok { $irods->add_collection_avu('/no_such_collection',
                                        'attr', 'value') }
    'Failed to add metadata to a non-existent collection';
}

sub remove_collection_avu : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $coll = "$irods_tmp_coll/irods";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  is($irods->remove_collection_avu($coll, 'b', 'x', 'cm'), $coll);
  is($irods->remove_collection_avu($coll, 'b', 'y'), $coll);

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_collection_meta($coll);

  is_deeply(\@observed_meta, $expected_meta,
            'Removed metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->remove_collecion_meta('/no_such_collection'
                                          , 'attr', 'value') }
    'Failed to remove metadata from a non-existent collection';
}

sub find_collections_by_meta : Test(7) {
  my $irods = WTSI::NPG::iRODS->new;

  my $expected_coll = "$irods_tmp_coll/irods";

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x'])],
            [$expected_coll]);

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x', '='])],
            [$expected_coll]);

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x'], ['a', 'y'])],
            [$expected_coll]);

  my $new_coll = "$irods_tmp_coll/irods/new";
  ok($irods->add_collection($new_coll));
  ok($irods->add_collection_avu($new_coll, 'a', 'x99'));

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x%', 'like'])],
            [$expected_coll, $new_coll]);

  dies_ok { $irods->find_collections_by_meta($irods_tmp_coll,
                                             ["a", "x", 'invalid_operator']) }
    'Expected to fail using an invalid query operator';
}

sub list_object : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_file = "$data_path/lorem.txt";
  my $lorem_object = "$irods_tmp_coll/lorem.txt";
  $irods->add_object($lorem_file, $lorem_object);

  is($irods->list_object($lorem_object), $lorem_object);

  my $lorem =  "$irods_tmp_coll/irods/lorem.txt";
  is($irods->list_object($lorem), $lorem);

  ok(!$irods->list_object('no_object_exists'),
     'Failed to list a non-existent object');

  dies_ok { $irods->list_object }
    'Failed to list an undefined object';
}

sub add_object : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_file = "$data_path/lorem.txt";
  my $lorem_object = "$irods_tmp_coll/lorem_added.txt";

  is($irods->add_object($lorem_file, $lorem_object), $lorem_object,
     'Added a data object');

  is($irods->list_object($lorem_object), $lorem_object,
    'Found a new data object');

  dies_ok { $irods->add_object }
    'Failed to add an undefined object';
}

sub replace_object : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_file = "$data_path/lorem.txt";
  my $to_replace = "$irods_tmp_coll/lorem_to_replace.txt";

  $irods->add_object($lorem_file, $to_replace);
  is($irods->replace_object($lorem_file, $to_replace), $to_replace,
     'Replaced a data object');

  # Improve this test by replacing with a different file and comparing
  # checksums

  dies_ok { $irods->replace_object($lorem_file, undef) }
    'Failed to replace an undefined object';
  dies_ok { $irods->replace_object(undef, $to_replace) }
    'Failed to replace an object with an undefined file';
}

sub move_object : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_file = "$data_path/lorem.txt";
  my $object_to_move = "$irods_tmp_coll/lorem_to_move.txt";
  my $object_moved = "$irods_tmp_coll/lorem_moved.txt";
  $irods->add_object($lorem_file, $object_to_move);

  is($irods->move_object($object_to_move, $object_moved), $object_moved,
     'Moved a data object');

  ok(!$irods->list_object($object_to_move), 'Object was moved 1');
  ok($irods->list_object($object_moved), 'Object was moved 2');

  dies_ok { $irods->move_object($object_to_move, undef) }
    'Failed to move an object to an undefined place';
  dies_ok { $irods->move_object(undef, $object_moved) }
    'Failed to move an undefined object';
}

sub remove_object : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  is($irods->remove_object($lorem_object), $lorem_object,
     'Removed a data object');
  ok(!$irods->list_object($lorem_object), 'Object was removed');

  dies_ok { $irods->remove_object('no_such_object') }
    'Failed to remove a non-existent object';
  dies_ok { $irods->remove_object }
    'Failed to remove an undefined object';
}

sub get_object_meta : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($lorem_object);

  is_deeply(\@observed_meta, $expected_meta,
            'Object metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->get_object_meta('/no_such_object', 'attr', 'value') }
    'Failed to get metadata from a non-existent object';
}

sub add_object_avu : Test(11) {
  my $irods = WTSI::NPG::iRODS->new;

  my $num_attrs = 8;
  my %meta = map { 'dattr' . $_ => 'dval' . $_ } 0 .. $num_attrs;

  my $test_object = "$irods_tmp_coll/irods/test.txt";
  foreach my $attr (keys %meta) {
    is($irods->add_object_avu($test_object, $attr, $meta{$attr}),
       $test_object);
  }

  my $expected_meta = [{attribute => 'dattr0', value => 'dval0'},
                       {attribute => 'dattr1', value => 'dval1'},
                       {attribute => 'dattr2', value => 'dval2'},
                       {attribute => 'dattr3', value => 'dval3'},
                       {attribute => 'dattr4', value => 'dval4'},
                       {attribute => 'dattr5', value => 'dval5'},
                       {attribute => 'dattr6', value => 'dval6'},
                       {attribute => 'dattr7', value => 'dval7'},
                       {attribute => 'dattr8', value => 'dval8'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($test_object);

  is_deeply(\@observed_meta, $expected_meta,
            'Object metadata added') or diag explain \@observed_meta;

  dies_ok { $irods->add_object_avu('/no_such_object', 'attr', 'value') }
    'Failed to add metadata to non-existent object';
}

sub remove_object_avu : Test(4) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  is($irods->remove_object_avu($lorem_object, 'b', 'x', 'cm'),
     $lorem_object);
  is($irods->remove_object_avu($lorem_object, 'b', 'y'),
     $lorem_object);

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($lorem_object);

  is_deeply(\@observed_meta, $expected_meta,
            'Removed metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->remove_object_avu('/no_such_object', 'attr', 'value') }
    'Failed to remove metadata from a non-existent object';
}

sub find_objects_by_meta : Test(6) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll, ['a', 'x'])],
            [$lorem_object]);

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll, ['a', 'x', '='])],
            [$lorem_object]);

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll,
                                          ['a', 'x'], ['a', 'y'])],
            [$lorem_object]);

  my $object = "$irods_tmp_coll/irods/test.txt";
  ok($irods->add_object_avu($object, 'a', 'x99'));

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll,
                                          ['a', 'x%', 'like'])],
            [$lorem_object, $object]);

  dies_ok { $irods->find_objects_by_meta($irods_tmp_coll,
                                         ["a", "x", 'invalid_operator']) }
    'Expected to fail using an invalid query operator';
}

sub calculate_checksum : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';

  is($irods->calculate_checksum($lorem_object), $expected_checksum,
     'Calculated checksum matched');
}

sub validate_checksum_metadata : Test(8) {
  my $irods = WTSI::NPG::iRODS->new;

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';
  my $invalid_checksum = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

  dies_ok { $irods->validate_checksum_metadata($lorem_object) }
    "Validation fails without metadata";

  ok($irods->add_object_avu($lorem_object, 'md5', $invalid_checksum));
  ok(!$irods->validate_checksum_metadata($lorem_object));
  ok($irods->remove_object_avu($lorem_object, 'md5', $invalid_checksum));

  ok($irods->add_object_avu($lorem_object, 'md5', $expected_checksum));
  ok($irods->validate_checksum_metadata($lorem_object), 'AVU checksum matched');

  ok($irods->add_object_avu($lorem_object, 'md5', $invalid_checksum));
  dies_ok { $irods->validate_checksum_metadata($lorem_object) }
    "Validation fails with multiple metadata values";
}

sub md5sum : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  is($irods->md5sum("$data_path/md5sum/lorem.txt"),
     '39a4aa291ca849d601e4e5b8ed627a04');
}

sub hash_path : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  is($irods->hash_path("$data_path/md5sum/lorem.txt"), '39/a4/aa');

  is($irods->hash_path("$data_path/md5sum/lorem.txt",
                       'aabbccxxxxxxxxxxxxxxxxxxxxxxxxxx'), 'aa/bb/cc');
}

sub round_trip_utf8_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;

  my $attr  = "Τη γλώσσα μου έδωσαν ελληνική";
  my $value = "το σπίτι φτωχικό στις αμμουδιές του Ομήρου";

  my $test_object = "$irods_tmp_coll/irods/test.txt";
  my @meta_before = $irods->get_object_meta($test_object);
  cmp_ok(scalar @meta_before, '==', 0, 'No AVUs present');

  my $expected_meta = [{attribute => $attr, value => $value}];
  ok($irods->add_object_avu($test_object, $attr, $value), 'UTF-8 AVU added');

  my @meta_after = $irods->get_object_meta($test_object);
  cmp_ok(scalar @meta_after, '==', 1, 'One AVU added');

  my $avu = $meta_after[0];
  TODO : {
    local $TODO = 'Temporarily disabled because of iRODS UTF-8 configuration';

    ok(Unicode::Collate->new->eq($avu->{attribute}, $attr),
       'Found UTF-8 attribute');
    ok(Unicode::Collate->new->eq($avu->{value}, $value),
       'Found UTF-8 value');
  }
}

sub slurp_object : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  my $test_file = "$data_path/utf-8.txt";
  my $test_object = "$irods_tmp_coll/irods/utf-8.txt";

  my $data = $irods->slurp_object($test_object);

  my $original = '';
  open my $fin, '<:encoding(utf-8)', $test_file or die "Failed to open $!\n";
  while (<$fin>) {
    $original .= $_;
  }
  close $fin;

  ok(Unicode::Collate->new->eq($data, $original), 'Slurped copy is identical');
}

1;
