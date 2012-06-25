
# Tests WTSI::Genotyping::iRODS

use utf8;

use strict;
use warnings;

use Data::Dumper;
use JSON;

use Test::More tests => 30;

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
                               remove_collection
                               get_collection_meta
                               add_collection_meta);

my $data_path = "t/irods";
my $test_file = "$data_path/test.txt";
my $test_dir = "$data_path/test";

my $test_collection = 'test_collection.' . $$;
my $test_object = 'test_object.' . $$;

my %meta = map { 'attribute' . $_ => 'value' . $_ } 0..9;
my %expected = map { $_ => [$meta{$_}] } keys %meta;

# add_collection
my $new_collection = add_collection($test_dir, $test_collection);
ok($new_collection);

# list_collection
my $wd = ipwd();
is_deeply([['file1.txt', 'file2.txt'],
           ["$wd/$test_collection/dir1", "$wd/$test_collection/dir2"]],
          [list_collection($new_collection)]);

# add_collection_meta
foreach my $attr (keys %meta) {
  my @x = add_collection_meta($test_collection, $attr, $meta{$attr});
  ok(scalar @x);
}

# get_collection_meta
my %collmeta = get_collection_meta($test_collection);
is_deeply(\%collmeta, \%expected);

ok(remove_collection($new_collection));

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
