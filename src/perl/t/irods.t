
# Tests WTSI::Genotyping::iRODS

use utf8;

use strict;
use warnings;

use JSON;

use Test::More tests => 49;
use Test::Exception;

BEGIN { use_ok('WTSI::Genotyping::iRODS'); }
require_ok('WTSI::Genotyping::iRODS');

use WTSI::Genotyping::iRODS qw(ipwd
                               list_object
                               add_object
                               remove_object
                               add_object_meta
                               get_object_meta
                               remove_object_meta

                               get_object_checksum
                               checksum_object

                               list_collection
                               add_collection
                               put_collection
                               remove_collection
                               get_collection_meta
                               add_collection_meta);

Log::Log4perl::init('etc/log4perl_tests.conf');

my $data_path = "t/irods";
my $test_file = "$data_path/test.txt";
my $test_dir = "$data_path/test";

my $test_collection = 'test_collection.' . $$;
my $test_object = 'test_object.' . $$;

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

# get_collection_meta
my %collmeta = get_collection_meta($test_collection);
is_deeply(\%collmeta, \%expected);

# add_object
dies_ok { add_object(undef, $test_object) }
  'Expected to fail adding a missing file as an object';
dies_ok { add_object($test_file, undef) }
  'Expected to fail adding a file as an undefined object';
my $new_object = add_object($test_file, $test_object);
ok($new_object);


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
dies_ok { get_object_meta() }
  'Expected to fail getting metdata for an undefined object';
is_deeply(\%objmeta, \%expected);

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

END {
  if ($dir1 && -d $dir1) {
    `rmdir $dir1`;
  }
  if ($dir2 && -d $dir2) {
    `rmdir $dir2`;
  }
}

1;
