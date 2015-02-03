
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);

use base qw(Test::Class);
use Test::More tests => 5;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $snpset = WTSI::NPG::Genotyping::SNPSet->new
  ('/nfs/gapi/data/genotype/pipeline_test/identity_check/W30467_snp_set_info_1000Genomes.tsv');

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::Identity');
}

sub get_num_samples : Test(1) {
  my $plink_path = "/nfs/gapi/data/genotype/pipeline_test/identity_check/identity_test";

  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  cmp_ok($check->get_num_samples, '==', 6, 'Number of samples')
}

sub get_sample_names : Test(1) {
  my $plink_path = "/nfs/gapi/data/genotype/pipeline_test/identity_check/identity_test";

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
  my $plink_path = "/nfs/gapi/data/genotype/pipeline_test/identity_check/identity_test";

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

sub get_plink_calls : Test(1) {
  my $plink_path = "/nfs/gapi/data/genotype/pipeline_test/identity_check/identity_test";

  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink  => plink_binary::plink_binary->new($plink_path),
     snpset => $snpset);

  $check->get_plink_calls;
}
