
# Tests WTSI::Genotyping::iRODS

use utf8;

use strict;
use warnings;

use Data::Dumper;
use JSON;

use Test::More tests => 15;

BEGIN { use_ok('WTSI::Genotyping::iRODS'); }
require_ok('WTSI::Genotyping::iRODS');

use WTSI::Genotyping::iRODS qw(irm

                               add_object
                               add_object_meta
                               get_object_meta
                               remove_object_meta

                               add_collection
                               get_collection_meta
                               add_collection_meta);

my $data_path = "t/irods";
my $test_file = "$data_path/test.txt";
my $test_target = 'test.' . $$;

is($test_target, add_object($test_file, $test_target));

my %meta = map { 'attribute' . $_ => 'value' . $_ } 0..9;

foreach my $attr (keys %meta) {
  my @x = add_object_meta($test_target, $attr, $meta{$attr});
  ok(scalar @x);
}

my %expected = map { $_ => [$meta{$_}] } keys %meta;
my %result = get_object_meta($test_target);
is_deeply(\%result, \%expected);

ok(irm($test_target));
