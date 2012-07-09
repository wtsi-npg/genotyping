
# Tests WTSI::Genotyping::iRODS

use utf8;

use strict;
use warnings;

use Data::Dumper;
use JSON;

use Test::More tests => 33;

BEGIN { use_ok('WTSI::Genotyping::iRODS'); }
require_ok('WTSI::Genotyping::iRODS');

use WTSI::Genotyping::iRODS qw(ipwd
                               list_object
                               add_object
                               remove_object
                               add_object_meta
                               get_object_meta
                               remove_object_meta

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
is(undef, list_collection($missing_collection));

# add_collection
my $added_collection = add_collection("$test_collection/added");
ok(list_collection($added_collection));

# remove_collection
ok(remove_collection($added_collection));
is(undef, list_collection($added_collection));

# put_collection
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
my $new_object = add_object($test_file, $test_object);
ok($new_object);

# list_object
is($test_object, list_object($test_object));

# add_object_meta
foreach my $attr (keys %meta) {
  my @x = add_object_meta($test_object, $attr, $meta{$attr});
  ok(scalar @x);
}

# get_object_meta
my %objmeta = get_object_meta($test_object);
is_deeply(\%objmeta, \%expected);

ok(remove_object($test_object));

ok(remove_collection($test_collection));
