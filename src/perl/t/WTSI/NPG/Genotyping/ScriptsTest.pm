
use utf8;

package WTSI::NPG::Genotyping::ScriptsTest;

use strict;
use warnings;
use File::Compare;
use File::Temp qw(tempdir);
use JSON;

use base qw(Test::Class);
use Test::More tests => 13;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $READY_PIPE     = './bin/ready_pipe.pl';
our $READY_INFINIUM = './bin/ready_infinium.pl';
our $READY_SAMPLES  = './bin/ready_samples.pl';

sub test_ready_pipe : Test(2) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_pipe.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);
  ok(-e "$dbfile");
}

sub test_ready_infinium : Test(8) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_infinium.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);

  my $run         = 'test';
  my $supplier    = 'wtsi';
  my $project     = 'coreex_bbgahs';
  my $qc_platform = 'Sequenom';

  ok(system("$READY_INFINIUM 2>/dev/null") != 0, 'Requires --dbfile');

  ok(system("$READY_INFINIUM --dbfile $dbfile " .
            "--supplier $supplier " .
            "--project '$project' " .
            "--qc-platform $qc_platform " .
            "2>/dev/null") != 0, 'Requires --run');

  ok(system("$READY_INFINIUM --dbfile $dbfile " .
            "--run $run " .
            "--project '$project' " .
            "--qc-platform $qc_platform " .
            "2>/dev/null") != 0, 'Requires --supplier');

  ok(system("$READY_INFINIUM --dbfile $dbfile " .
            "--run $run " .
            "--supplier $supplier " .
            "--qc-platform $qc_platform " .
            "2>/dev/null") != 0, 'Requires --project');

  ok(system("$READY_INFINIUM --dbfile $dbfile " .
            "--run $run " .
            "--supplier $supplier " .
            "--project '$project' " .
            "2>/dev/null") != 0, 'Requires --qc-platform');

  ok(system("$READY_INFINIUM --dbfile $dbfile " .
            "--run $run " .
            "--supplier $supplier " .
            "--project '$project' " .
            "--qc-platform $qc_platform " .
            "--maximum 10") == 0, 'Basic use');
  ok(-e "$dbfile");
}

sub test_ready_samples : Test(3) {
  my $tmpdir = tempdir(CLEANUP => 1);
  my $dbfile = "$tmpdir/test_ready_samples.db";

  ok(system("$READY_PIPE --dbfile $dbfile") == 0);

  my $run         = 'test';
  my $supplier    = 'wtsi';
  my $project     = 'coreex_bbgahs';
  my $qc_platform = 'Sequenom';

  ok(system("$READY_INFINIUM --dbfile $dbfile " .
            "--run $run " .
            "--supplier $supplier " .
            "--project '$project' " .
            "--qc-platform $qc_platform " .
            "--maximum 10") == 0);

  ok(system("$READY_SAMPLES --dbfile $dbfile") == 0, 'List samples');
}

1;
