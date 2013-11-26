
use utf8;

package WTSI::NPG::UtilitiesTest;

use strict;
use warnings;
use File::Temp qw(tempfile);

use base qw(Test::Class);
use Test::More tests => 21;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Utilities'); }

use WTSI::NPG::Utilities qw(common_stem
                            collect_dirs
                            collect_files
                            modified_between);

my $data_path = './t/utilities';

sub test_common_stem : Test(11) {
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
}

sub test_collect_files : Test(4) {

  # Accept all files
  my $file_test = sub {
    my ($file) = @_;
    return 1;
  };

  my $collect_path = "$data_path/collect_files";

  is_deeply([collect_files($collect_path, $file_test, 1)],
            []);

  is_deeply([collect_files($collect_path, $file_test, 2)],
            ["$collect_path/a/10.txt",
             "$collect_path/b/20.txt",
             "$collect_path/c/30.txt"]);

  is_deeply([collect_files($collect_path, $file_test, 3)],
            ["$collect_path/a/10.txt",
             "$collect_path/a/x/1.txt",
             "$collect_path/b/20.txt",
             "$collect_path/b/y/2.txt",
             "$collect_path/c/30.txt",
             "$collect_path/c/z/3.txt"]);

  is_deeply([collect_files($collect_path, $file_test, undef)],
            ["$collect_path/a/10.txt",
             "$collect_path/a/x/1.txt",
             "$collect_path/b/20.txt",
             "$collect_path/b/y/2.txt",
             "$collect_path/c/30.txt",
             "$collect_path/c/z/3.txt"]);
}

sub test_collect_dirs : Test(4) {

  # Accept all dirs
  my $dir_test = sub {
    my ($dir) = @_;
    return 1;
  };

  my $collect_path = "$data_path/collect_files";

  is_deeply([collect_dirs($collect_path, $dir_test, 1)],
            ["$collect_path"]);

  is_deeply([collect_dirs($collect_path, $dir_test, 2)],
            ["$collect_path",
             "$collect_path/a",
             "$collect_path/b",
             "$collect_path/c"]);

  is_deeply([collect_dirs($collect_path, $dir_test, 3)],
            ["$collect_path",
             "$collect_path/a",
             "$collect_path/a/x",
             "$collect_path/b",
             "$collect_path/b/y",
             "$collect_path/c",
             "$collect_path/c/z"]);

  is_deeply([collect_dirs($collect_path, $dir_test, undef)],
            ["$collect_path",
             "$collect_path/a",
             "$collect_path/a/x",
             "$collect_path/b",
             "$collect_path/b/y",
             "$collect_path/c",
             "$collect_path/c/z"]);
}

sub test_modified_between : Test(1) {
  my $then = DateTime->now;
  my ($fh, $file) = tempfile();
  my $now = DateTime->now;

  my $fn = modified_between($then->epoch, $now->epoch);
  ok($fn->($file));
}
