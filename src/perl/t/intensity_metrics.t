# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

use strict;
use warnings;
use Carp;
use Digest::MD5;
use FindBin qw($Bin);
use File::Temp qw(tempdir);
use Test::More tests => 5;

my $simPath = "$Bin/qc_test_data/small_test.sim";
my $expectedMag = '1353405268f863b7f4eb691d1cca5c68';
my $expectedXY = 'f439ac9d58104e243b1cb9602ab0933b';
my $temp = tempdir("test_int_metric_XXXXXX", CLEANUP => 1 );
# force inline C to recompile into empty directory
my $inline = $temp."/inline_dir";
mkdir($inline);
$ENV{'PERL_INLINE_DIRECTORY'} = $inline;

my $outPathMag = $temp."/mag.txt";
my $outPathXY = $temp."/xyd.txt";
my $simNull = $temp."/foo.sim"; # does not exist!
my $magNull = $temp."/foo_mag.txt";
my $xydNull = $temp."/foo_xyd.txt";
my $verbose = 0;
my $bin = "$Bin/../bin";

my $cmd = "$bin/print_simfile_header.pl $simPath > /dev/null";
is(0, system($cmd), "Print .sim header contents (output to /dev/null)");

$cmd = "$bin/intensity_metrics.pl --input $simPath --magnitude $outPathMag ".
    "--xydiff $outPathXY";
is(0, system($cmd), "Intensity metric script exit status");

ok(md5match($outPathMag, $expectedMag), "MD5 checksum for magnitude");

ok(md5match($outPathXY, $expectedXY), "MD5 checksum for xydiff");

$cmd = "$bin/intensity_metrics.pl --input $simNull --magnitude $magNull ".
    "--xydiff $xydNull &> /dev/null";
isnt(0, system($cmd), "Intensity metric script exit status (missing input)");

sub md5match {
    my $inPath = shift;
    my $expected = shift;
    my $md5 = Digest::MD5->new;
    open my $fh, "<", $inPath || croak "Cannot open MD5 input $inPath";
    binmode($fh);
    while (<$fh>) { $md5->add($_); }
    close $fh || croak "Cannot close MD5 input $inPath";
    if ($md5->hexdigest eq $expected) { return 1; }
    else { return 0; }
}
