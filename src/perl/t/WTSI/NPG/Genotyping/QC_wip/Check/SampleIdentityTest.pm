
package WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityTest;

use strict;
use warnings;
use File::Slurp qw(read_file);
use File::Temp qw(tempdir);
use JSON;

use Data::Dumper; # TODO remove when development is stable

use base qw(Test::Class);
use Test::More tests => 4;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";
my $json_file = "$data_path/expected_sample_identity.json";
my $pass_threshold = 0.9;
my $sample_name  = 'urn:wtsi:249442_C09_HELIC5102247';
my ($qc_calls, $production_calls);

sub setup : Test(setup) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    # Some fake QC data; x denotes a call mismatch when compared to the
    # Plink data for the same sample
    my @qc_data = (['GS34251',    'TT'],
                   ['GS35205',    'TT'],
                   ['GS35219',    'TT'],
                   ['GS35220',    'CC'],
                   ['rs649058',   'GA'], # AG (sample data)
                   ['rs1131498',  'AA'],
                   ['rs1805087',  'AA'], # AG x
                   ['rs3795677',  'TT'], # AG x
                   ['rs6166',     'GA'], # AG
                   ['rs1801262',  'AA'],
                   ['rs2286963',  'GT'], # GT
                   ['rs6759892',  'GT'], # GT
                   ['rs7627615',  'GA'], # AG
                   ['rs11096957', 'AA'],
                   ['rs2247870',  'TT'], # CT x
                   ['rs4619',     'AG'], # AG
                   ['rs532841',   'CT'], # CT
                   ['rs6557634',  'CT'], # CT
                   ['rs4925',     'AA'], # AC x
                   ['rs156697',   'AA'],
                   ['rs5215',     'CT'], # CT
                   ['rs12828016', 'AA'],
                   ['rs7298565',  'GA'], # AG
                   ['rs3742207',  'AC'], # AC
                   ['rs4075254',  'CT'], # CT
                   ['rs4843075',  'GA'], # AG
                   ['rs8065080',  'CT'], # CT
                   ['rs1805034',  'AA'],
                   ['rs2241714',  'GA'], # CT
                   ['rs753381',   'AG']  # AG
               );

    my @qc_calls = map {
        my ($snp, $genotype) = @$_;

        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @qc_data;

    $qc_calls = \@qc_calls;

    # as above, but with the original Plink calls
    my @production_data =  (['GS34251',    'TT'],
                            ['GS35205',    'TT'],
                            ['GS35219',    'TT'],
                            ['GS35220',    'CC'],
                            ['rs649058',   'AG'],
                            ['rs1131498',  'AA'],
                            ['rs1805087',  'AG'],
                            ['rs3795677',  'AG'],
                            ['rs6166',     'AG'],
                            ['rs1801262',  'AA'],
                            ['rs2286963',  'GT'],
                            ['rs6759892',  'GT'],
                            ['rs7627615',  'AG'],
                            ['rs11096957', 'AA'],
                            ['rs2247870',  'CT'],
                            ['rs4619',     'AG'],
                            ['rs532841',   'CT'],
                            ['rs6557634',  'CT'],
                            ['rs4925',     'AC'],
                            ['rs156697',   'AA'],
                            ['rs5215',     'CT'],
                            ['rs12828016', 'AA'],
                            ['rs7298565',  'AG'],
                            ['rs3742207',  'AC'],
                            ['rs4075254',  'CT'],
                            ['rs4843075',  'GA'],
                            ['rs8065080',  'CT'],
                            ['rs1805034',  'AA'],
                            ['rs2241714',  'CT'],
                            ['rs753381',   'AG']
                        );
    my @production_calls = map {
        my ($snp, $genotype) = @$_;

        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @production_data;

    $production_calls = \@production_calls;

}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity');
}

sub construct : Test(1) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my @args = (sample_name         => $sample_name,
                snpset              => $snpset,
                production_calls    => $production_calls,
                qc_calls            => $qc_calls,
                pass_threshold      => $pass_threshold);

    new_ok('WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity' => \@args);
}

sub json_spec : Test(1) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my %args = (sample_name         => $sample_name,
                snpset              => $snpset,
                production_calls    => $production_calls,
                qc_calls            => $qc_calls,
                pass_threshold      => $pass_threshold);
    my $sample_id = WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->
        new(\%args);
    my $json_spec = $sample_id->to_json_spec();
    my $expected = decode_json(read_file($json_file));
    is_deeply($json_spec, $expected,
              'JSON spec congruent with expected values');
}


sub swap_metric : Test(1) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my %args = (sample_name         => $sample_name,
                snpset              => $snpset,
                production_calls    => $production_calls,
                qc_calls            => $qc_calls,
                pass_threshold      => $pass_threshold);
    my $sample_id = WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->
        new(\%args);

    # create a second SampleIdentity object and run the swap check
    my $other_name = 'fake_swapped_sample';
    my @other_qc_data = (['GS34251',    'TT'],
                         ['GS35205',    'TT'],
                         ['GS35219',    'TT'],
                         ['GS35220',    'CC'],
                         ['rs649058',   'AG'],
                         ['rs1131498',  'AA'],
                         ['rs1805087',  'AG'],
                         ['rs3795677',  'AG'],
                         ['rs6166',     'AG'],
                         ['rs1801262',  'AA'],
                         ['rs2286963',  'GT'],
                         ['rs6759892',  'GT'],
                         ['rs7627615',  'AG'],
                         ['rs11096957', 'AA'],
                         ['rs2247870',  'CT'],
                         ['rs4619',     'AG'],
                         ['rs532841',   'CT'],
                         ['rs6557634',  'CT'],
                         ['rs4925',     'AC'],
                         ['rs156697',   'AA'],
                         ['rs5215',     'CT'],
                         ['rs12828016', 'AA'],
                         ['rs7298565',  'AG'],
                         ['rs3742207',  'AC'],
                         ['rs4075254',  'CT'],
                         ['rs4843075',  'GA'],
                         ['rs8065080',  'CT'],
                         ['rs1805034',  'AA'],
                         ['rs2241714',  'CT'],
                         ['rs753381',   'AA'] # was AG
                     ); # almost identical to 'main' production data
    my @other_qc_calls = map {
        my ($snp, $genotype) = @$_;

        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @other_qc_data;

    my @other_prod_data = (['GS34251',    'TT'],
                           ['GS35205',    'TT'],
                           ['GS35219',    'TT'],
                           ['GS35220',    'CC'],
                           ['rs649058',   'GA'],
                           ['rs1131498',  'AA'],
                           ['rs1805087',  'AA'],
                           ['rs3795677',  'TT'],
                           ['rs6166',     'GA'],
                           ['rs1801262',  'AA'],
                           ['rs2286963',  'GT'],
                           ['rs6759892',  'GT'],
                           ['rs7627615',  'GA'],
                           ['rs11096957', 'AA'],
                           ['rs2247870',  'TT'],
                           ['rs4619',     'AG'],
                           ['rs532841',   'CT'],
                           ['rs6557634',  'CT'],
                           ['rs4925',     'AA'],
                           ['rs156697',   'AA'],
                           ['rs5215',     'CT'],
                           ['rs12828016', 'AA'],
                           ['rs7298565',  'GA'],
                           ['rs3742207',  'AC'],
                           ['rs4075254',  'CT'],
                           ['rs4843075',  'GA'],
                           ['rs8065080',  'CT'],
                           ['rs1805034',  'AA'],
                           ['rs2241714',  'GG'], # was GA
                           ['rs753381',   'AG']
                       ); # almost identical to 'main' QC data

    my @other_prod_calls = map {
        my ($snp, $genotype) = @$_;

        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @other_prod_data;

    my %other_args = (sample_name         => $sample_name,
                      snpset              => $snpset,
                      production_calls    => \@other_prod_calls,
                      qc_calls            => \@other_qc_calls,
                      pass_threshold      => $pass_threshold);
    my $other_id = WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->
        new(\%other_args);

    my $swap = $sample_id->find_swap_metric($other_id);
    ok(abs($swap - 0.96666667) < 0.0000001,
       "Swap metric matches expected value");
}


1;
