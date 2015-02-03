
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);

use base qw(Test::Class);
use Test::More tests => 22;
use Test::Exception;

use plink_binary;
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

sub get_plink_calls : Test(18) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  my $calls = $check->get_plink_calls;

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
