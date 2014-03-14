
# Tests write_snp_metadata.pl

use strict;
use warnings;
use Carp;
use Cwd qw/abs_path/;
use Digest::MD5;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use JSON;
use Test::More tests => 5;

my $bin = "$Bin/../bin/";
my $manifest = "/nfs/gapi/data/genotype/pipeline_test/manifests/".
    "Human670-QuadCustom_v1_A_TRUNCATED.bpm.csv";
my $temp = tempdir("snp_meta_test_XXXXXX", CLEANUP => 1);
my $chr = $temp."/chr.json";
my $snp = $temp."/snp.json";

my $cmd = "$bin/write_snp_metadata.pl --manifest $manifest --chromosomes $chr ".
    "--snp $snp";
is(0, system($cmd), "$bin/write_snp_metadata.pl exit status");

my ($md5, $fh);
$md5 = Digest::MD5->new;
open $fh, "<", $snp || croak "Cannot open SNP JSON $snp";
binmode($fh);
while (<$fh>) { $md5->add($_); }
close $fh || croak "Cannot close SNP JSON $snp";
is($md5->hexdigest, 'fb7ee3090f5c28315af02a7b52887749', 
   "MD5 checksum of SNP JSON");

my ($json, $in);
open $in, "<", $snp || croak "Cannot open SNP JSON $snp for reading";
while (<$in>) { $json .= $_; }
close $in || croak "Cannot close SNP JSON $snp after reading";
ok(decode_json($json), "Parse SNP JSON output");

$md5 = Digest::MD5->new;
open $fh, "<", $chr || croak "Cannot open chromosome JSON $chr";
binmode($fh);
while (<$fh>) { $md5->add($_); }
close $fh || croak "Cannot close chromosome JSON $chr";
is($md5->hexdigest, '60be79bee72459670a5ab33419533546', 
   "MD5 checksum of chromosome JSON");

$json="";
open $in, "<", $chr || croak "Cannot open chromosome JSON $chr for reading";
while (<$in>) { $json .= $_; }
close $in || croak "Cannot close chromosome JSON $chr after reading";
ok(decode_json($json), "Parse chromsome JSON output");
