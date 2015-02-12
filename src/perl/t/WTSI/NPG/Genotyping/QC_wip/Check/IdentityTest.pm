
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);
use JSON;
use List::AllUtils qw(each_array);

use Data::Dumper; # TODO remove when development is stable

use base qw(Test::Class);
use Test::More tests => 77;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $plink_path = "$data_path/fake_qc_genotypes";
my $plink_swap = "$data_path/fake_swap_genotypes";
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::Identity');
}

sub get_num_samples : Test(1) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  cmp_ok($check->get_num_samples, '==', 6, 'Number of samples')
}

sub get_sample_names : Test(1) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my @expected = ('urn:wtsi:000000_A00_DUMMY-SAMPLE',
                  'urn:wtsi:249441_F11_HELIC5102138',
                  'urn:wtsi:249442_C09_HELIC5102247',
                  'urn:wtsi:249461_G12_HELIC5215300',
                  'urn:wtsi:249469_H06_HELIC5274668',
                  'urn:wtsi:249470_F02_HELIC5274730');

  my $names = $check->get_sample_names;
  is_deeply($names, \@expected) or diag explain $names;
}

sub get_shared_snp_names : Test(1) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my @expected = ('rs1805087',
                  'rs2241714',
                  'rs2247870',
                  'rs2286963',
                  'rs3742207',
                  'rs3795677',
                  'rs4075254',
                  'rs4619',
                  'rs4843075',
                  'rs4925',
                  'rs5215',
                  'rs532841',
                  'rs6166',
                  'rs649058',
                  'rs6557634',
                  'rs6759892',
                  'rs7298565',
                  'rs753381',
                  'rs7627615',
                  'rs8065080');

  my $shared = $check->get_shared_snp_names;
  is_deeply($shared, \@expected) or diag explain $shared;
}

sub get_all_calls : Test(18) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my $calls = $check->get_all_calls;

  # Map of sample name to Plink genotypes
  my $expected_genotypes =
    {'urn:wtsi:000000_A00_DUMMY-SAMPLE' => [('NN') x 20],

     'urn:wtsi:249441_F11_HELIC5102138' =>
     ['AG', 'AG', 'CT', 'GT', 'AC', 'AG', 'CT', 'AG', 'AG', 'AC',
      'CT', 'CT', 'AG', 'AG', 'CT', 'GT', 'AG', 'AG', 'AG', 'CT'],

     'urn:wtsi:249442_C09_HELIC5102247' =>
     ['AG', 'AG', 'CT', 'GT', 'AC', 'AG', 'CT', 'AG', 'AG', 'AC',
      'CT', 'CT', 'AG', 'AG', 'CT', 'GT', 'AG', 'AG', 'AG', 'CT'],

     'urn:wtsi:249461_G12_HELIC5215300' =>
     [('NN') x 10,
      'CT', 'CT', 'AG', 'AG', 'CT', 'GT', 'AG', 'AG', 'AG', 'CT'],

     'urn:wtsi:249469_H06_HELIC5274668' =>
     ['AG', 'AG', 'CT', 'GT', 'AC', 'AG', 'CT', 'AG', 'AG', 'AC',
      'CT', 'CT', 'AG', 'AG', 'CT', 'GT', 'AG', 'AG', 'AG', 'CT'],

     'urn:wtsi:249470_F02_HELIC5274730' =>
     ['AG', 'AG', 'CT', 'GT', 'AC', 'AG', 'CT', 'AG', 'AG', 'AC',
      'CT', 'CT', 'AG', 'AG', 'CT', 'GT', 'AG', 'AG', 'AG', 'CT']};

  my @expected_snp_names = @{$check->get_shared_snp_names};

  foreach my $sample_name (@{$check->get_sample_names}) {
    my @calls = @{$calls->{$sample_name}};

    cmp_ok(scalar @calls, '==', 20, "Number of $sample_name calls");

    my @snp_names = map { $_->snp->name } @calls;
    is_deeply(\@snp_names, \@expected_snp_names) or diag explain \@snp_names;

    my @genotypes = map { $_->genotype } @calls;
    is_deeply(\@genotypes, $expected_genotypes->{$sample_name})
      or diag explain \@genotypes;
  }
}

sub get_sample_calls : Test(14) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my @sample_names = ('urn:wtsi:000000_A00_DUMMY-SAMPLE',
                      'urn:wtsi:249441_F11_HELIC5102138',
                      'urn:wtsi:249442_C09_HELIC5102247',
                      'urn:wtsi:249461_G12_HELIC5215300',
                      'urn:wtsi:249469_H06_HELIC5274668',
                      'urn:wtsi:249470_F02_HELIC5274730');

  foreach my $name (@sample_names) {
    my $calls = $check->get_sample_calls($name);
    ok($calls, "$name calls");
    cmp_ok(scalar @$calls, '==', 20, "Number of $name calls");
  }

  my @expected_snp_names = @{$check->get_shared_snp_names};
  my $expected_genotypes =
    [('NN') x 10,
     'CT', 'CT', 'AG', 'AG', 'CT', 'GT', 'AG', 'AG', 'AG', 'CT'];

  my $sample_name  = 'urn:wtsi:249461_G12_HELIC5215300';
  my @calls = @{$check->get_sample_calls($sample_name)};

  my @snp_names = map { $_->snp->name } @calls;
  is_deeply(\@snp_names, \@expected_snp_names) or diag explain \@snp_names;

  my @genotypes = map { $_->genotype } @calls;
  is_deeply(\@genotypes, $expected_genotypes) or diag explain \@genotypes;
}

sub pair_sample_calls : Test(5) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my $sample_name  = 'urn:wtsi:249442_C09_HELIC5102247';

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

  my @pairs = @{$check->pair_sample_calls($sample_name, \@qc_calls)};
  my @matches    = grep {  $_->{qc}->equivalent($_->{sample}) } @pairs;
  my @mismatches = grep { !$_->{qc}->equivalent($_->{sample}) } @pairs;
  cmp_ok(scalar @pairs,      '==', 20, 'Number of pairs');
  cmp_ok(scalar @matches,    '==', 16, 'Number of matches');
  cmp_ok(scalar @mismatches, '==', 4,  'Number of mismatches');

  # Expected match SNP list (retains QC order)
  my @expected_matches = ('rs649058',
                          'rs6166',
                          'rs2286963',
                          'rs6759892',
                          'rs7627615',
                          'rs4619',
                          'rs532841',
                          'rs6557634',
                          'rs5215',
                          'rs7298565',
                          'rs3742207',
                          'rs4075254',
                          'rs4843075',
                          'rs8065080',
                          'rs2241714',
                          'rs753381');
  my @matched_snps = map { $_->{qc}->snp->name } @matches;
  is_deeply(\@matched_snps, \@expected_matches)
    or diag explain \@matched_snps;

  # Expected mismatched SNP list (retains QC order)
  my @expected_mismatches = ('rs1805087',
                             'rs3795677',
                             'rs2247870',
                             'rs4925');
  my @mismatched_snps = map { $_->{qc}->snp->name } @mismatches;
  is_deeply(\@mismatched_snps, \@expected_mismatches)
    or diag explain \@mismatched_snps;
}

sub count_sample_matches : Test(6) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my @qc_data = (['rs649058',   'GA'],
                 ['rs1805087',  'AG'],
                 ['rs3795677',  'AG'],
                 ['rs6166',     'GA'],
                 ['rs2286963',  'GT'],
                 ['rs6759892',  'GT'],
                 ['rs7627615',  'GA'],
                 ['rs2247870',  'GA'],
                 ['rs4619',     'AG'],
                 ['rs532841',   'CC']);

  my @qc_calls = map {
    my ($snp, $genotype) = @$_;

    WTSI::NPG::Genotyping::Call->new
        (snp      => $snpset->named_snp($snp),
         genotype => $genotype) } @qc_data;

  my $matches = $check->count_sample_matches
    ('urn:wtsi:249441_F11_HELIC5102138', \@qc_calls);

  my @expected_matched_snps = ('rs649058',
                               'rs1805087',
                               'rs3795677',
                               'rs6166',
                               'rs2286963',
                               'rs6759892',
                               'rs7627615',
                               'rs2247870',
                               'rs4619');
  ok(!$matches->{failed}, 'Sample not failed');
  cmp_ok($matches->{identity} * 10, '==', 9, 'Fraction identical');

  cmp_ok((scalar @{$matches->{match}}), '==', 9,
         'Number of calls matched');

  my @matched_snps = map { $_->{qc}->snp->name } @{$matches->{match}};
  is_deeply(\@matched_snps, \@expected_matched_snps,
            'Expected matched SNPS') or diag explain \@matched_snps;

  my @expected_mismatched_snps = ('rs532841');
  cmp_ok((scalar @{$matches->{mismatch}}), '==', 1,
         'Number of calls mismatched');

  my @mismatched_snps = map { $_->{qc}->snp->name } @{$matches->{mismatch}};
  is_deeply(\@mismatched_snps, \@expected_mismatched_snps,
            'Expected mismatched SNPS') or diag explain \@matched_snps;
}

sub report_all_matches : Test(25) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my @sample_names = ('urn:wtsi:000000_A00_DUMMY-SAMPLE',
                      'urn:wtsi:249441_F11_HELIC5102138',
                      'urn:wtsi:249442_C09_HELIC5102247',
                      'urn:wtsi:249461_G12_HELIC5215300',
                      'urn:wtsi:249469_H06_HELIC5274668',
                      'urn:wtsi:249470_F02_HELIC5274730');
  # Extract pairs for just 2 SNPS from every sample
  my @qc_data = (['rs1805087',  'AG'],
                 ['rs3795677',  'TT']);

  my @all_qc_calls;
  foreach my $sample_name (@sample_names) {
    my @qc_calls = map {
      my ($snp, $genotype) = @$_;

      WTSI::NPG::Genotyping::Call->new
          (snp      => $snpset->named_snp($snp),
           genotype => $genotype) } @qc_data;

    push @all_qc_calls, {sample => $sample_name,
                         calls  => \@qc_calls};
  }

  my $expected =
    {'urn:wtsi:000000_A00_DUMMY-SAMPLE' =>
      {'failed'    => 1,
       'genotypes' => {'rs1805087' => ['AG', 'NN'],
                       'rs3795677' => ['TT', 'NN']},
       'identity'  => 0,
       'missing'   => 0,
       'sample'    => 'urn:wtsi:000000_A00_DUMMY-SAMPLE'
      },

     'urn:wtsi:249441_F11_HELIC5102138' =>
      {'failed'    => 1,
       'genotypes' => {'rs1805087' => ['AG', 'AG'],
                       'rs3795677' => ['TT', 'AG']},
       'identity'  => 0.5,
       'missing'   => 0,
       'sample'    => 'urn:wtsi:249441_F11_HELIC5102138'
      },

     'urn:wtsi:249442_C09_HELIC5102247' =>
     {'failed'    => 1,
      'genotypes' => {'rs1805087' => ['AG', 'AG'],
                      'rs3795677' => ['TT', 'AG']},
      'identity'  => 0.5,
      'missing'   => 0,
      'sample'    => 'urn:wtsi:249442_C09_HELIC5102247'
     },

     'urn:wtsi:249461_G12_HELIC5215300' =>
     {'failed'    => 1,
      'genotypes' => {'rs1805087' => ['AG', 'NN'],
                      'rs3795677' => ['TT', 'NN']},
      'identity'  => 0,
      'missing'   => 0,
      'sample'    => 'urn:wtsi:249461_G12_HELIC5215300'
     },
     'urn:wtsi:249469_H06_HELIC5274668' =>
     {
      'failed'    => 1,
      'genotypes' => {'rs1805087' => ['AG', 'AG'],
                      'rs3795677' => ['TT', 'AG']},
      'identity' => 0.5,
      'missing'  => 0,
      'sample'    => 'urn:wtsi:249469_H06_HELIC5274668'
     },

     'urn:wtsi:249470_F02_HELIC5274730' =>
     {
      'failed'    => 1,
      'genotypes' => {'rs1805087' => ['AG', 'AG'],
                      'rs3795677' => ['TT', 'AG']},
      'identity'  => 0.5,
      'missing'   => 0,
      'sample'    => 'urn:wtsi:249470_F02_HELIC5274730'
     }
    };

  my $json = $check->report_all_matches(\@all_qc_calls);
  my $result = decode_json($json);

  is_deeply($result, $expected) or diag explain $result;
}

sub sample_swap_evaluation : Test(2) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_swap,
         snpset     => $snpset);

    my $samples_qc_calls = _get_qc_swap_calls();
    my $compared = $check->evaluate_sample_swaps($samples_qc_calls);
    ok($compared, "Failed pair comparison completed");

    my @expected = (
        [
            'urn:wtsi:249442_C09_HELIC5102247',
            'urn:wtsi:249441_F11_HELIC5102138',
            1,
            1
        ],
        [
            'urn:wtsi:249461_G12_HELIC5215300',
            'urn:wtsi:249441_F11_HELIC5102138',
            0.5,
            0
        ],
        [
            'urn:wtsi:249461_G12_HELIC5215300',
            'urn:wtsi:249442_C09_HELIC5102247',
            0.8,
            0
        ]
    );
    is_deeply($compared, \@expected, "Comparison matches expected values");
}

sub run_identity_check : Test(3) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_swap,
         snpset     => $snpset);

    # construct the input data structure
    my @qc_call_sets = ();
    foreach my $sample_qc (@{_get_qc_swap_calls()}) {
        my ($sample, $qc_calls) = @{$sample_qc};
        my %call_set = ('sample' => $sample,
                        'calls'  => $qc_calls);
        push(@qc_call_sets, \%call_set);
    }

    # run the identity check
    my $combined_results = $check->run_identity_check(\@qc_call_sets);
    ok($combined_results, 'Combined identity & swap check OK');
    #print Dumper $combined_results;

    my $combined_json = $check->combined_results_to_json($combined_results);
    ok($combined_json, 'Combined results to JSON OK');

    my $expected_json = '{"identity":[{"identity":0,"missing":0,"sample":"urn:wtsi:249441_F11_HELIC5102138","genotypes":{"rs6557634":["CT","TT"],"rs6759892":["GT","TT"],"rs753381":["AG","GG"],"rs7627615":["GA","GG"],"rs6166":["AG","GG"],"rs8065080":["CT","TT"],"rs4075254":["CT","TT"],"rs4925":["AC","CC"],"rs1805087":["AG","GG"],"rs532841":["CT","TT"],"rs2286963":["GT","TT"],"rs2247870":["CT","TT"],"rs649058":["AG","GG"],"rs2241714":["CT","GG"],"rs3742207":["AC","CC"],"rs3795677":["AG","GG"],"rs7298565":["AG","GG"],"rs5215":["CT","TT"],"rs4619":["AG","GG"],"rs4843075":["AG","GG"]},"failed":"1"},{"identity":0,"missing":0,"sample":"urn:wtsi:249442_C09_HELIC5102247","genotypes":{"rs6557634":["TT","CT"],"rs6759892":["TT","GT"],"rs753381":["GG","AG"],"rs7627615":["GG","AG"],"rs6166":["GG","AG"],"rs8065080":["TT","CT"],"rs4075254":["TT","CT"],"rs4925":["CC","AC"],"rs1805087":["GG","AG"],"rs532841":["TT","CT"],"rs2286963":["TT","GT"],"rs2247870":["TT","CT"],"rs649058":["GG","AG"],"rs2241714":["TT","AG"],"rs3742207":["CC","AC"],"rs3795677":["GG","AG"],"rs7298565":["GG","AG"],"rs5215":["TT","CT"],"rs4619":["GG","AG"],"rs4843075":["GG","AG"]},"failed":"1"},{"identity":0.5,"missing":0,"sample":"urn:wtsi:249461_G12_HELIC5215300","genotypes":{"rs6557634":["CT","CT"],"rs6759892":["GT","GT"],"rs753381":["AG","AG"],"rs7627615":["GA","AG"],"rs6166":["GA","AG"],"rs4075254":["CT","NN"],"rs4925":["AA","NN"],"rs1805087":["AA","NN"],"rs8065080":["CT","CT"],"rs532841":["CT","CT"],"rs2286963":["GT","NN"],"rs2247870":["TT","NN"],"rs649058":["GA","AG"],"rs2241714":["GA","NN"],"rs3742207":["AC","NN"],"rs3795677":["TT","NN"],"rs7298565":["GA","AG"],"rs5215":["CT","CT"],"rs4619":["AG","NN"],"rs4843075":["GA","NN"]},"failed":"1"}],"swap_comparison":[["urn:wtsi:249442_C09_HELIC5102247","urn:wtsi:249441_F11_HELIC5102138",0,0],["urn:wtsi:249461_G12_HELIC5215300","urn:wtsi:249441_F11_HELIC5102138",0,0],["urn:wtsi:249461_G12_HELIC5215300","urn:wtsi:249442_C09_HELIC5102247",0.5,0]]}';
    is($combined_json, $expected_json, 'Combined JSON matches expected');
}



sub _get_qc_swap_calls {

    # Some fake QC data
    # - List of 3 'failed' sample names, 2 of which are swapped
    # - Fake QC calls for each sample

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my @samples = qw/urn:wtsi:249441_F11_HELIC5102138
                     urn:wtsi:249442_C09_HELIC5102247
                     urn:wtsi:249461_G12_HELIC5215300/;
    my %qc_data = (
        # swap with urn:wtsi:249442_C09_HELIC5102247
      $samples[0] =>
            [ ['GS34251',    'TT'],
              ['GS35205',    'TT'],
              ['GS35219',    'TT'],
              ['GS35220',    'CC'],
              ['rs649058',   'AG'],
              ['rs1131498',  'AA'],
              ['rs1805087',  'AG'],
              ['rs3795677',  'AG'], # AG
              ['rs6166',     'AG'], # AG
              ['rs1801262',  'AA'],
              ['rs2286963',  'GT'], # GT
              ['rs6759892',  'GT'], # GT
              ['rs7627615',  'GA'], # AG
              ['rs11096957', 'AA'],
              ['rs2247870',  'CT'], # CT
              ['rs4619',     'AG'], # AG
              ['rs532841',   'CT'], # CT
              ['rs6557634',  'CT'], # CT
              ['rs4925',     'AC'], # AC
              ['rs156697',   'AA'],
              ['rs5215',     'CT'], # CT
              ['rs12828016', 'AA'],
              ['rs7298565',  'AG'], # AG
              ['rs3742207',  'AC'], # AC
              ['rs4075254',  'CT'], # CT
              ['rs4843075',  'AG'], # AG
              ['rs8065080',  'CT'], # CT
              ['rs1805034',  'AA'],
              ['rs2241714',  'CT'], # CT
              ['rs753381',   'AG']  # AG
          ],
        # swap with urn:wtsi:249441_F11_HELIC5102138
      $samples[1] =>
            [ ['GS34251',    'TT'],
              ['GS35205',    'TT'],
              ['GS35219',    'TT'],
              ['GS35220',    'CC'],
              ['rs649058',   'GG'], # AG
              ['rs1131498',  'AA'],
              ['rs1805087',  'GG'], # AG
              ['rs3795677',  'GG'], # AG
              ['rs6166',     'GG'], # AG
              ['rs1801262',  'AA'],
              ['rs2286963',  'TT'], # GT
              ['rs6759892',  'TT'], # GT
              ['rs7627615',  'GG'], # AG
              ['rs11096957', 'AA'],
              ['rs2247870',  'TT'], # CT
              ['rs4619',     'GG'], # AG
              ['rs532841',   'TT'], # CT
              ['rs6557634',  'TT'], # CT
              ['rs4925',     'CC'], # AC
              ['rs156697',   'AA'],
              ['rs5215',     'TT'], # CT
              ['rs12828016', 'AA'],
              ['rs7298565',  'GG'], # AG
              ['rs3742207',  'CC'], # AC
              ['rs4075254',  'TT'], # CT
              ['rs4843075',  'GG'], # AG
              ['rs8065080',  'TT'], # CT
              ['rs1805034',  'AA'],
              ['rs2241714',  'TT'], # CT
              ['rs753381',   'GG']  # AG
          ],
        # 'ordinary' failed sample
        # x denotes a mismatch wrt Plink data
      $samples[2] =>
            [ ['GS34251',    'TT'],
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
            ]
    );

    my @samples_qc_calls;
    foreach my $sample (@samples) {
        my @qc_calls =  map {
            my ($snp, $genotype) = @$_;
            WTSI::NPG::Genotyping::Call->new
                  (snp      => $snpset->named_snp($snp),
                   genotype => $genotype) } @{$qc_data{$sample}};
        push(@samples_qc_calls, [ $sample, [ @qc_calls ] ]);
    }
    return \@samples_qc_calls;
}
