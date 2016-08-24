use utf8;

package WTSI::NPG::CollectorTest;

use strict;
use warnings;
use File::Temp qw(tempfile);

use base qw(WTSI::NPG::Test);
use Test::More tests => 11;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Utilities::Collector'); }

use WTSI::NPG::Utilities::Collector;

my $data_path = './t/collector';

sub test_collect_files : Test(4) {

  # Accept all files
  my $file_test = sub {
    my ($file) = @_;
    return 1;
  };

  my $collect_path = "$data_path/collect_files";

  my @depths = (1, 2, 3, undef);
  my @expected = (
      [],
      ["$collect_path/a/10.txt",
       "$collect_path/b/20.txt",
       "$collect_path/c/30.txt"],
      ["$collect_path/a/10.txt",
       "$collect_path/a/x/1.txt",
       "$collect_path/b/20.txt",
       "$collect_path/b/y/2.txt",
       "$collect_path/c/30.txt",
       "$collect_path/c/z/3.txt"],
      ["$collect_path/a/10.txt",
       "$collect_path/a/x/1.txt",
       "$collect_path/b/20.txt",
       "$collect_path/b/y/2.txt",
       "$collect_path/c/30.txt",
       "$collect_path/c/z/3.txt"]
  );
  for (my $i=0;$i<@depths;$i++) {
      my $collector = WTSI::NPG::Utilities::Collector->new(
          root  => $collect_path,
          depth => $depths[$i],
      );
      is_deeply([$collector->collect_files($file_test)],
                $expected[$i]);
  }
}

sub test_collect_dirs : Test(5) {

  # Accept all dirs
  my $dir_test = sub {
    my ($dir) = @_;
    return 1;
  };

  my $collect_path = "$data_path/collect_files";

  my @depths = (1, 2, 3, undef);
  my @expected = (
      ["$collect_path"],
      ["$collect_path",
       "$collect_path/a",
       "$collect_path/b",
       "$collect_path/c"],
      ["$collect_path",
       "$collect_path/a",
       "$collect_path/a/x",
       "$collect_path/b",
       "$collect_path/b/y",
       "$collect_path/c",
       "$collect_path/c/z"],
      ["$collect_path",
       "$collect_path/a",
       "$collect_path/a/x",
       "$collect_path/b",
       "$collect_path/b/y",
       "$collect_path/c",
       "$collect_path/c/z"]
  );
  for (my $i=0;$i<@depths;$i++) {
      my $collector = WTSI::NPG::Utilities::Collector->new(
          root  => $collect_path,
          depth => $depths[$i],
      );
      is_deeply([$collector->collect_dirs($dir_test)],
                $expected[$i]);
  }

  # collect with regex
  my $collector = WTSI::NPG::Utilities::Collector->new(
      root  => $collect_path,
      depth => 2,
      regex => qr/^[ab]$/msx);
  is_deeply([$collector->collect_dirs($dir_test)],
            ["$collect_path/a",
             "$collect_path/b"]);
}

sub test_modified_between : Test(1) {
  my $then = DateTime->now;
  my ($fh, $file) = tempfile();
  my $now = DateTime->now;
  my $collect_path = "$data_path/collect_files";

  my $collector = WTSI::NPG::Utilities::Collector->new(
      root => $collect_path,
  );

  my $fn = $collector->modified_between($then->epoch, $now->epoch);
  ok($fn->($file));
}
