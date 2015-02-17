
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityResultsTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 10;
use Test::Exception;

#use Data::Dumper; # TODO remove when development is stable

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::IdentityResults;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::IdentityResults');
}

sub add_remove_results : Test(9) {

    # make an empty results container
    my $results =
        WTSI::NPG::Genotyping::QC_wip::Check::IdentityResults->new();

    # create some calls
    my $sample = 'fake_sample_name';
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my @sample_data = (['rs649058',   'GA'], # AG (sample data)
                       ['rs1131498',  'AA'],
                       ['rs1805087',  'GG'], # AG x
                       ['rs3795677',  'AG'],
                   );

    my @qc_data = (['rs649058',   'AG'], # AG (sample data)
                   ['rs1131498',  'AA'],
                   ['rs1805087',  'AG'],
                   ['rs3795677',  'AG'], # AG x
                   );
    my @sample_calls = map {
        my ($snp, $genotype) = @$_;
        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @sample_data;
    my @qc_calls = map {
        my ($snp, $genotype) = @$_;
        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @qc_data;
    # now build a results hash
    my (@matches, @mismatches);
    foreach my $i (0, 2) {
        push @matches, { sample => $sample_calls[$i],
                         qc => $qc_calls[$i] };
    }
    foreach my $i (1, 3) {
        push @mismatches, { sample => $sample_calls[$i],
                            qc => $qc_calls[$i] };
    }

    my $result = { match    => \@matches,
                   mismatch => \@mismatches,
                   identity => 0.5,
                   missing  => 0,
                   failed   => 1 };

    is($results->get_num_samples, 0, 'Count starts at zero');
    ok($results->add_sample_result($sample, $result), 'Added a result');
    is($results->get_num_samples, 1, 'Count updated correctly');
    dies_ok(sub { $results->add_sample_result($sample, $result) },
            'Cannot add same sample twice');
    # TODO separate test for get_failed_results and to_json_spec ?
    my $failed = $results->get_failed_results();
    isa_ok($failed, 'WTSI::NPG::Genotyping::QC_wip::Check::IdentityResults',
       'Failed results object');
    is($failed->get_num_samples, 1, "Correct number of failed results");
    my $results_json = $results->to_json_spec();
    my $expected_json = [
          [
            'fake_sample_name',
            {
              'identity' => '0.5',
              'missing' => 0,
              'genotypes' => {
                               'rs3795677' => [
                                                'AG',
                                                'AG'
                                              ],
                               'rs1131498' => [
                                                'AA',
                                                'AA'
                                              ],
                               'rs649058' => [
                                               'AG',
                                               'GA'
                                             ],
                               'rs1805087' => [
                                                'AG',
                                                'GG'
                                              ]
                             },
              'failed' => 1
            }
          ]
      ];
    is_deeply($results_json, $expected_json,
              "Results in JSON format match expected value");
    # now test deleting a result
    ok($results->delete_sample_result($sample), 'Deleted a result');
    is($results->get_num_samples, 0, 'Count updated correctly');
}
