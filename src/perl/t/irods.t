
# Tests WTSI::Genotyping::iRODS

use utf8;

use strict;
use warnings;

use DateTime;
use File::Temp qw(tempfile);
use JSON;

use Test::More tests => 92;
use Test::Exception;

BEGIN { use_ok('WTSI::NPG::iRODS'); }
require_ok('WTSI::NPG::iRODS');

use WTSI::NPG::iRODS qw(ipwd
                        list_object
                        add_object
                        remove_object
                        add_object_meta
                        get_object_meta
                        remove_object_meta
                        find_objects_by_meta

                        get_object_checksum
                        checksum_object

                        list_collection
                        add_collection
                        put_collection
                        remove_collection
                        get_collection_meta
                        add_collection_meta
                        find_collections_by_meta

                        group_exists
                        add_group
                        set_group_access

                        collect_files
                        collect_dirs
                        modified_between
                        md5sum
                        hash_path
                        meta_exists);

Log::Log4perl::init('etc/log4perl_tests.conf');

my $data_path = "t/irods";
my $test_file = "$data_path/test.txt";
my $test_dir = "$data_path/test";

# Deliberate spaces in names
my $test_collection = 'test_ _collection.' . $$;
my $test_object = 'test_ _object.' . $$;

my %meta = map { 'attribute' . $_ => 'value' . $_ } 0..9;
my %expected = map { $_ => [$meta{$_}] } keys %meta;

# list_collection
my $missing_collection  = $test_collection . '/no/such/collection/exists';
is(list_collection($missing_collection), undef);

# add_collection
my $added_collection = add_collection("$test_collection/added");
ok(list_collection($added_collection));

# remove_collection
ok(remove_collection($added_collection));
is(list_collection($added_collection), undef);

# put_collection
my $dir1 = join q[/], $test_dir, 'dir1';
my $dir2 = join q[/], $test_dir, 'dir2';
mkdir $dir1;
mkdir $dir2;
my $put_collection = put_collection($test_dir, $test_collection);
ok($put_collection);

my $wd = ipwd();
is_deeply([list_collection($put_collection)],
          [['file1.txt', 'file2.txt'],
           ["$wd/$test_collection/test/dir1",
            "$wd/$test_collection/test/dir2"]]);

# add_collection_meta
foreach my $attr (keys %meta) {
  my @x = add_collection_meta($test_collection, $attr, $meta{$attr});
  ok(scalar @x);
}

# meta_exists
my %m = ('a' => ['1', '2'],
         'b' => ['1'],
         'c' => ['2']);

ok(meta_exists('a', '1', %m));
ok(meta_exists('a', '2', %m));
is(meta_exists('a', '12', %m), 0);
is(meta_exists('a', '11', %m), 0);


# get_collection_meta
my %collmeta = get_collection_meta($test_collection);
is_deeply(\%collmeta, \%expected);

# find_collections_by_meta
foreach my $attr (keys %meta) {
  my $value = $meta{$attr};
  my $root = $wd;
  my @found = find_collections_by_meta("$root%", $attr, $value);

  ok(scalar @found == 1);
}


# add_object
dies_ok { add_object(undef, $test_object) }
  'Expected to fail adding a missing file as an object';
dies_ok { add_object($test_file, undef) }
  'Expected to fail adding a file as an undefined object';
my $new_object = add_object($test_file, $test_object);
ok($new_object);

# set_group_access
dies_ok { set_group_access('no_such_permission', 'public', $new_object) }
  'Expected to fail setting access with an invalid permission argument';
dies_ok { set_group_access('read', 'no_such_group_exists', $new_object) }
  'Expected to fail setting access for non-existant group';
ok(set_group_access('read', 'public', $new_object));
ok(set_group_access(undef, 'public', $new_object));

# list_object
dies_ok { list_object() }
  'Expected to fail listing an undefined object';
is(list_object($test_object), $test_object);

# add_object_meta
dies_ok { add_object_meta('no_such_object', 'attr', 'value') }
  'Expected to fail adding metadata to non-existent object';

foreach my $attr (keys %meta) {
  my @x = add_object_meta($test_object, $attr, $meta{$attr});
  ok(scalar @x);
}

# get_object_meta
my %objmeta = get_object_meta($test_object);
is_deeply(\%objmeta, \%expected);

dies_ok { get_object_meta() }
  'Expected to fail getting metdata for an undefined object';

# find_objects_by_meta
foreach my $attr (keys %meta) {
  my $value = $meta{$attr};
  my $root = $wd;
  my @found = find_objects_by_meta("$root%", $attr, $value);

  ok(scalar @found == 1);
}


# remove_object
dies_ok { remove_object() }
  'Expected to fail removing an undefined object';
ok(remove_object($test_object));

# remove_collection
dies_ok { remove_collection() }
  'Expected to fail removing an undefined collection';
ok(remove_collection($test_collection));


my $lorem_file = "$data_path/lorem.txt";
my $lorem_object = 'lorem_object.' . $$;

ok(add_object($lorem_file, $lorem_object));

# get_object_checksum
my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';
my $invalid_checksum = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';
is(get_object_checksum($lorem_object), $expected_checksum);

is_deeply([add_object_meta($lorem_object, 'md5', $invalid_checksum)],
          ['md5', $invalid_checksum, '']);

# checksum_object
ok(! checksum_object($lorem_object));

is_deeply([remove_object_meta($lorem_object, 'md5', $invalid_checksum)],
          ['md5', $invalid_checksum, '']);

is_deeply([add_object_meta($lorem_object, 'md5', $expected_checksum)],
          ['md5', $expected_checksum, '']);

ok(checksum_object($lorem_object));

ok(remove_object($lorem_object));


# group exists
ok(group_exists('rodsadmin'));
ok(!group_exists('no_such_group_exists'));

# A hack to see if there are admin rights for the following tests. I can't
# find a clean way to list these rights in iRODS.

my $no_admin = system("iadmin mkgroup foo 2>&1 | grep -E '^ERROR.*SYS_NO_API_PRIV'") == 0;

if ($no_admin) {
  dies_ok { add_group('rodsadmin') }
    'Expected to fail due to lack of permission';
  dies_ok { add_group('test_group') }
    'Expected to fail due to lack of permission';
  dies_ok { remove_group('test_group') }
    'Expected to fail due to lack of permission';
}
else {
  # add_group
  dies_ok { add_group('rodsadmin') }
    'Expected to fail adding a group that exists already';
  ok(add_group('test_group'));
  ok(remove_group('test_group'));
}

# collect_files
my $test = sub {
  my ($file) = @_;
  return 1;
};

my $collect_path = "$data_path/collect_files";

is_deeply([collect_files($collect_path, $test, 1)],
          []);

is_deeply([collect_files($collect_path, $test, 2)],
          ["$collect_path/a/10.txt",
           "$collect_path/b/20.txt",
           "$collect_path/c/30.txt"]);

is_deeply([collect_files($collect_path, $test, 3)],
          ["$collect_path/a/10.txt",
           "$collect_path/a/x/1.txt",
           "$collect_path/b/20.txt",
           "$collect_path/b/y/2.txt",
           "$collect_path/c/30.txt",
           "$collect_path/c/z/3.txt"]);

# collect_dirs
is_deeply([collect_dirs($collect_path, $test, 1)],
          ["$collect_path"]);

is_deeply([collect_dirs($collect_path, $test, 2)],
          ["$collect_path",
           "$collect_path/a",
           "$collect_path/b",
           "$collect_path/c"]);

is_deeply([collect_dirs($collect_path, $test, 3)],
          ["$collect_path",
           "$collect_path/a",
           "$collect_path/a/x",
           "$collect_path/b",
           "$collect_path/b/y",
           "$collect_path/c",
           "$collect_path/c/z"]);

# modified_between
my $then = DateTime->now;
my ($fh, $file) = tempfile();
my $now = DateTime->now;

my $fn = modified_between($then->epoch, $now->epoch);
ok($fn->($file));

# md5sum
is(md5sum("$data_path/md5sum/lorem.txt"), '39a4aa291ca849d601e4e5b8ed627a04');

# hash_path
is(hash_path("$data_path/md5sum/lorem.txt"), '39/a4/aa');

is(hash_path("$data_path/md5sum/lorem.txt", 'aabbccxxxxxxxxxxxxxxxxxxxxxxxxxx'), 'aa/bb/cc');


END {
  if ($dir1 && -d $dir1) {
    `rmdir $dir1`;
  }
  if ($dir2 && -d $dir2) {
    `rmdir $dir2`;
  }
}

1;
