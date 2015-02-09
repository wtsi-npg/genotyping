
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);
use List::AllUtils qw(each_array);

use base qw(Test::Class);
use Test::More tests => 90;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $plink_path = "$data_path/fake_qc_genotypes";
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

sub pair_all_calls : Test(25) {
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
  my @qc_data = (['rs1805087',  'AA'],
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

  my @paired = @{$check->pair_all_calls(\@all_qc_calls)};
  cmp_ok(scalar @paired, '==', 6, 'Number of paired sets');

  my $ea = each_array(@sample_names, @paired);
  while (my ($sample_name, $pairs) = $ea->()) {
    is($pairs->{sample}, $sample_name, "Name for $sample_name");

    my @pairs = @{$pairs->{pairs}};
    cmp_ok(scalar @pairs, '==', 2, "Pairs for $sample_name");
    is($pairs[0]->{qc}->snp->name, 'rs1805087', "SNP 0 for $sample_name");
    is($pairs[1]->{qc}->snp->name, 'rs3795677', "SNP 1 for $sample_name");
  }
}
