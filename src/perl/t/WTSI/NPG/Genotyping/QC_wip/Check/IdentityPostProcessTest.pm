
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcessTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 4;
use Test::Exception;

use JSON;
use File::Slurp qw/read_file/;

use WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $json_result_path = "$data_path/expected_identity_results.json";
my $expected_csv = "$data_path/expected_identity_merged.csv";
my %inputs = (qc_A => $json_result_path,
              qc_B => $json_result_path,
              qc_C => $json_result_path,
          );

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess');
}

sub write_csv: Test(2) {
    my $outPath = '/tmp/genotype_merge_test.csv';
    my $processor = WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess->new();
    ok($processor->run(\%inputs, $outPath), "Run method to write CSV");
    ok(-e $outPath, "Output $outPath exists");
    # TODO read output and check against reference CSV
}

sub create_csv_data: Test(1) {
    my $expected_csv_fields = _read_csv($expected_csv);
    my %allResults;
    foreach my $resultName (keys(%inputs)) {
        my $result = from_json(read_file($inputs{$resultName}));
        $allResults{$resultName} = $result;
    }
    my $processor = WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess->new();
    my $csv_fields_ref = $processor->mergeGenotypes(\%allResults);
    is_deeply($csv_fields_ref, $expected_csv_fields, 'CSV fields match');
}

sub _read_csv {
    my ($inPath, ) = @_;
    my @csv_lines = read_file($inPath);
    my @csv_fields;
    foreach my $line (@csv_lines) {
        chomp $line;
        my @fields = split(/,/, $line);
        push(@csv_fields, \@fields);
    }
    return \@csv_fields;
}
