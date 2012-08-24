
# Tests WTSI::Genotyping::Utilities

use utf8;

use strict;
use warnings;
use DateTime;
use File::Temp qw(tempfile);
use Test::More tests => 21;

use WTSI::Genotyping qw(common_stem
                        collect_files
                        collect_dirs
                        modified_between
                        md5sum
                        hash_path);

Log::Log4perl::init('etc/log4perl_tests.conf');

my $data_path = "t/utilities";

# common_stem
is(common_stem('', ''), '');
is(common_stem('a', ''), '');
is(common_stem('', 'a'), '');
is(common_stem('a', 'a'), 'a');

is(common_stem('aa', 'a'), 'a');
is(common_stem('a', 'aa'), 'a');
is(common_stem('ab', 'a'), 'a');
is(common_stem('a', 'ab'), 'a');

is(common_stem('aa', 'aa'), 'aa');
is(common_stem('aa', 'bb'), '');

is(common_stem('abc123', 'abc456'), 'abc');

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
