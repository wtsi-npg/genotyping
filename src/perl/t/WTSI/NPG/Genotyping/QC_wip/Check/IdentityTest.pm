
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);
use File::Slurp qw(read_file);
use JSON;
use List::AllUtils qw(each_array);

use Data::Dumper; # TODO remove when development is stable

use base qw(Test::Class);
use Test::More tests => 26;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $plink_path = "$data_path/fake_qc_genotypes";
my $plink_swap = "$data_path/fake_swap_genotypes";
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";
my $expected_json_path = "$data_path/expected_identity_results.json";
my $pass_threshold = 0.9;

# sample names with fake QC data
my @qc_sample_names = qw/urn:wtsi:249441_F11_HELIC5102138
                         urn:wtsi:249442_C09_HELIC5102247
                         urn:wtsi:249461_G12_HELIC5215300/;

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::Identity');
}

sub find_identity : Test(2) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);
  # get fake QC results for a few samples; others will appear as missing
  my $qc_callsets = _get_qc_callsets();
  my $id_results = $check->find_identity($qc_callsets);
  ok($id_results, "Find identity results for given QC calls");
  my @json_spec_results;
  foreach my $id_result (@{$id_results}) {
      push(@json_spec_results, $id_result->to_json_spec());
  }
  my $expected_json = decode_json(read_file($expected_json_path));
  is_deeply(\@json_spec_results, $expected_json,
            "JSON output congruent with expected values");
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

sub get_production_calls : Test(18) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my $calls = $check->get_production_calls;

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

sub sample_swap_evaluation : Test(2) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_swap,
         snpset     => $snpset);

    my $sample_ids = _get_swap_sample_identities();
    my $compared = $check->pairwise_swap_check($sample_ids);
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

sub _get_swap_sample_identities {

    # Some fake QC data
    # - List of 3 'failed' sample names, 2 of which are swapped
    # - Create SampleIdentity object for each sample

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_swap,
         snpset     => $snpset);
    my $qc_callsets = _get_qc_callsets();
    my $all_production_calls = $check->get_production_calls();

    my @sample_identities;
    my @qc_callsets = _get_qc_callsets();
    foreach my $sample_name (@qc_sample_names) {
        # need both QC and production calls to create a SampleIdentity object
        my %args = (sample_name      => $sample_name,
                    snpset           => $snpset,
                    production_calls => $all_production_calls->{$sample_name},
                    qc_calls         => $qc_callsets->{$sample_name},
                    pass_threshold   => $pass_threshold);
        my $sample_id = WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentity->
            new(\%args);
        push (@sample_identities, $sample_id);
    }
    return \@sample_identities;
}

sub _get_qc_callsets {

   # Some fake QC data
    # - List of 3 'failed' sample names, 2 of which are swapped
    # - Create a hash of Call arrays

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my %qc_data = (
        # swap with urn:wtsi:249442_C09_HELIC5102247
      $qc_sample_names[0] =>
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
      $qc_sample_names[1] =>
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
      $qc_sample_names[2] =>
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

    my %qc_callsets;
    foreach my $sample_name (@qc_sample_names) {
        my @qc_calls =  map {
            my ($snp, $genotype) = @$_;
            WTSI::NPG::Genotyping::Call->new
                  (snp      => $snpset->named_snp($snp),
                   genotype => $genotype) } @{$qc_data{$sample_name}};
        $qc_callsets{$sample_name} = \@qc_calls;
    }
    return \%qc_callsets;
}

1;
