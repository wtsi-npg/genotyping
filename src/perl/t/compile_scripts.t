
use strict;
use warnings;

use Test::More;

eval "use Test::Compile";
plan skip_all => "Test::Compile required for testing compilation"
  if $@;

my $test = Test::Compile->new();
$test->verbose(0);
$test->all_files_ok($test->all_pl_files('bin'));
$test->done_testing();

1;
