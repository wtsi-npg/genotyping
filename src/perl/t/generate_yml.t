
# Tests generate_yml.pl

use strict;
use warnings;
use Carp;
use Digest::MD5;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use Test::More tests => 11;

my $bin = "$Bin/../bin/";
my $temp = tempdir("generate_yml_test_XXXXXX", CLEANUP => 1);
chdir($temp);

print "TEMPDIR: $temp\n";

my $config = "config.yml";
my ($md5, $fh);
my @workflows = qw/null genosnp illuminus zcall/;
my %checksums = ('config'    => '579c6b2688c37b8fc42bf45cb633c287',
		 'genosnp'   => '8317ee4faad759ee0e75749de0fe63f4',
		 'illuminus' => '7828416db4837d026f17c39e6bbfef9d',
		 'zcall'     => '64a33d0a79b522f8d35d2bec5d333c10'
    );
# use checksums to validate yml output
# original files are in t/generate_yml if needed for troubleshooting

my $cmd_root = "$bin/generate_yml.pl --run run1 --workdir /home/foo/dummy_workdir";
foreach my $workflow (@workflows) {
    my $cmd;
    if ($workflow eq 'null') { $cmd = $cmd_root; }
    else { $cmd = $cmd_root." --workflow $workflow --manifest /home/foo/dummy_manifest.bpm.csv"; }
    if ($workflow eq 'zcall') { $cmd .= ' --egt /home/foo/dummy_cluster.egt'; }
    is(0,system($cmd),"$bin/generate_yml.pl exit status, $workflow workflow");
    # checksum of config.yml
    $md5 = Digest::MD5->new;
    open $fh, "<", $config || croak "Cannot open config YML $config";
    binmode($fh);
    while (<$fh>) { $md5->add($_); }
    close $fh || croak "Cannot close config YML $config";
    is($md5->hexdigest, $checksums{'config'}, "MD5 checksum of config YML, $workflow workflow");
    # checksum of workflow .yml (if any)
    if ($workflow ne 'null') {
	my $output = "genotype_".$workflow.".yml";
	open $fh, "<", $output || croak "Cannot open workflow YML $output";
	binmode($fh);
	while (<$fh>) { $md5->add($_); }
	close $fh || croak "Cannot close workflow YML $output";
	is($md5->hexdigest, $checksums{$workflow}, "MD5 checksum of workflow YML, workflow $workflow");
    }
}
