
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);

use base qw(Test::Class);
use Test::More tests => 30;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $plink_path = "$data_path/identity_test";
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::Identity');
}

sub get_num_samples : Test(1) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  cmp_ok($check->get_num_samples, '==', 6, 'Number of samples')
}

sub get_sample_names : Test(1) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

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
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

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
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  my $calls = $check->get_all_calls;

  # Map of sample name to Plink genotypes
  my $expected_genotypes =
    {'urn:wtsi:000000_A00_DUMMY-SAMPLE' => [('NN') x 20],

     'urn:wtsi:249441_F11_HELIC5102138' =>
     ['AA', 'AG', 'TT', 'TT', 'AC', 'AG', 'CT', 'AA', 'GA', 'AA',
      'CC', 'TT', 'AG', 'GG', 'CC', 'GG', 'AA', 'GG', 'GA', 'CT'],

     'urn:wtsi:249442_C09_HELIC5102247' =>
     ['GG', 'GG', 'CC', 'NN', 'CC', 'AA', 'CT', 'GG', 'GA', 'AA',
      'CT', 'TT', 'AG', 'GA', 'TT', 'GG', 'GA', 'AG', 'AA', 'CT'],

     'urn:wtsi:249461_G12_HELIC5215300' =>
     [('NN') x 13, 'GA', 'TC', 'TG', 'AA', 'GG', 'GA', 'TT'],

     'urn:wtsi:249469_H06_HELIC5274668' =>
     ['AA', 'AG', 'TT', 'TT', 'AC', 'GG', 'TT', 'AA', 'AA', 'CC',
      'TT', 'CT', 'AG', 'GA', 'TC', 'TG', 'AA', 'GG', 'AA', 'TT'],

     'urn:wtsi:249470_F02_HELIC5274730' =>
     ['GA', 'GG', 'TT', 'TT', 'AC', 'GG', 'TT', 'GG', 'AA', 'CA',
      'CT', 'CT', 'AG', 'AA', 'CC', 'TT', 'GA', 'AA', 'AA', 'CT']};

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

sub get_sample_calls : Test(3) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  my $sample_name  = 'urn:wtsi:249461_G12_HELIC5215300';
  my @calls = @{$check->get_sample_calls($sample_name)};

  cmp_ok(scalar @calls, '==', 20, "Number of $sample_name calls");

  my @expected_snp_names = @{$check->get_shared_snp_names};
  my $expected_genotypes =
    [('NN') x 13, 'GA', 'TC', 'TG', 'AA', 'GG', 'GA', 'TT'],;

  my @snp_names = map { $_->snp->name } @calls;
  is_deeply(\@snp_names, \@expected_snp_names) or diag explain \@snp_names;

  my @genotypes = map { $_->genotype } @calls;
  is_deeply(\@genotypes, $expected_genotypes) or diag explain \@genotypes;
}

sub compare_calls : Test(3) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  my $sample_name  = 'urn:wtsi:249442_C09_HELIC5102247';

  # Some fake QC data; x denotes a call mismatch when compared to the
  # Plink data for the same sample
  my @qc_data = (['GS34251',    'TT'],
                 ['GS35205',    'TT'],
                 ['GS35219',    'TT'],
                 ['GS35220',    'CC'],
                 ['rs649058',   'GA'], # GA
                 ['rs1131498',  'AA'],
                 ['rs1805087',  'AA'], # GG x
                 ['rs3795677',  'TT'], # AA
                 ['rs6166',     'GA'], # AG
                 ['rs1801262',  'AA'],
                 ['rs2286963',  'TT'], # NN x
                 ['rs6759892',  'CC'], # GG
                 ['rs7627615',  'GA'], # AA x
                 ['rs11096957', 'AA'],
                 ['rs2247870',  'TT'], # CC x
                 ['rs4619',     'GG'], # GG
                 ['rs532841',   'TT'], # TT
                 ['rs6557634',  'AA'], # TT
                 ['rs4925',     'AA'], # AA
                 ['rs156697',   'AA'],
                 ['rs5215',     'CT'], # CT
                 ['rs12828016', 'AA'],
                 ['rs7298565',  'GA'], # GA
                 ['rs3742207',  'AC'], # CC x
                 ['rs4075254',  'CT'], # CT
                 ['rs4843075',  'GA'], # GA
                 ['rs8065080',  'CT'], # CT
                 ['rs1805034',  'AA'],
                 ['rs2241714',  'GA'], # GG x
                 ['rs753381',   'AG']  # AG
                 );

  my @qc_calls = map {
    my ($snp, $genotype) = @$_;

    WTSI::NPG::Genotyping::Call->new
        (snp      => $snpset->named_snp($snp),
         genotype => $genotype) } @qc_data;

  my @comparisons = @{$check->compare_calls($sample_name, \@qc_calls)};
  my @matches    = grep {  $_->{equivalent} } @comparisons;
  my @mismatches = grep { !$_->{equivalent} } @comparisons;
  cmp_ok(scalar @comparisons, '==', 20, 'Number of comparisons');
  cmp_ok(scalar @matches,     '==', 14, 'Number of matches');
  cmp_ok(scalar @mismatches,  '==', 6,  'Number of mismatches');

  # Expected match SNP list (retains QC order)
  my @expected_matches = ('rs649058',
                          'rs3795677',
                          'rs6166',
                          'rs6759892',
                          'rs4619',
                          'rs532841',
                          'rs6557634',
                          'rs4925',
                          'rs5215',
                          'rs7298565',
                          'rs4075254',
                          'rs4843075',
                          'rs8065080',
                          'rs753381');
  my @matched_snps = map { $_->{qc}->snp->name } @matches;
  is_deeply(\@matched_snps, \@expected_matches)
    or diag explain \@matched_snps;

  # Expected mismatched SNP list (retains QC order)
  my @expected_mismatches = ('rs1805087',
                             'rs2286963',
                             'rs7627615',
                             'rs2247870',
                             'rs3742207',
                             'rs2241714');
  my @mismatched_snps = map { $_->{qc}->snp->name } @mismatches;
  is_deeply(\@mismatched_snps, \@expected_mismatches)
    or diag explain \@mismatched_snps;
}
