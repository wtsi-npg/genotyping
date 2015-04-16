
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcessTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 8;
use Test::Exception;

use JSON;
use File::Slurp qw/read_file/;
use File::Temp qw/tempdir/;
use Text::CSV;

use WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $json_path = "$data_path/expected_identity_results.json";
my $json_path_mod = "$data_path/expected_identity_results_modified.json";
my $expected_csv = "$data_path/expected_identity_merged.csv";
my %inputs = (qc_A => $json_path,
              qc_B => $json_path,
              qc_C => $json_path_mod,
          );
# expected_identity_results.json is also used to test Identity.pm.
# expected_identity_results_modified.json differs as follows:
# - New fake SNP, rs000000_DUMMY-SNP
# - For sample urn:wtsi:249441_F11_HELIC5102138, all AG calls on QC plex
# changed to AA
# - For sample urn:wtsi:249442_C09_HELIC5102247, all calls on SNPs starting
# rs[456] changed to NN
# - identity scores changed to be consistent with modified calls

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess');
}

sub write_csv: Test(4) {
    my $workDir = tempdir('temp_identity_XXXXXX', CLEANUP=>1);
    my $outPath = $workDir.'/genotype_merge_test.csv';
    new_ok('WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess');
    my $processor =
        WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess->new();
    ok($processor->mergeIdentityFiles(\%inputs, $outPath),
       "Run method to write CSV");
    ok(-e $outPath, "Output $outPath exists");
    my $expected_csv_fields = _read_csv($expected_csv);
    my $output_csv_fields = _read_csv($outPath);
    is_deeply($output_csv_fields, $expected_csv_fields,
              'CSV fields match from output file');
}

sub create_csv_data: Test(1) {
    my $expected_csv_fields = _read_csv($expected_csv);
    my %allResults;
    foreach my $resultName (keys(%inputs)) {
        my $result = from_json(read_file($inputs{$resultName}));
        $allResults{$resultName} = $result;
    }
    my $processor =
        WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess->new();
    my $csv_fields_ref = $processor->_mergeGenotypes(\%allResults);
    is_deeply($csv_fields_ref, $expected_csv_fields,
              'CSV fields match from data structure');
}

sub csv_script: Test(2) {
    # TODO maybe move this into Scripts.pm (which is very slow to run)
    my $workDir = tempdir('temp_identity_script_XXXXXX', CLEANUP=>1);
    my $write_csv_script = "./bin/write_identity_csv.pl";
    my @names = sort(keys(%inputs));
    my $outPath = $workDir."/merged_identity.csv";
    ok(system(join q{ }, "$write_csv_script",
              "--in ".$inputs{$names[0]},
              "--name ".$names[0],
              "--in ".$inputs{$names[1]},
              "--name ".$names[1],
              "--in ".$inputs{$names[2]},
              "--name ".$names[2],
              "--out ".$outPath
          ) == 0, 'Ran identity CSV script');
    my $expected_csv_fields = _read_csv($expected_csv);
    my $output_csv_fields = _read_csv($outPath);
    is_deeply($output_csv_fields, $expected_csv_fields,
              'CSV fields match from output file');

}

sub _read_csv {
    my ($inPath, ) = @_;
    my $csv = Text::CSV->new({eol              => "\n",
                              sep_char         => ",",
                              allow_whitespace => undef,
                              quote_char       => undef});
    open my $in, "<", $inPath || logcroak("Cannot open input '$inPath'");
    my $csv_fields_ref = $csv->getline_all($in);
    close $in || logcroak("Cannot close input '$inPath'");
    return $csv_fields_ref;
}
