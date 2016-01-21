
package WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest;

use strict;
use warnings;
use File::Temp qw(tempdir);
use File::Slurp qw(read_file);
use JSON;
use List::AllUtils qw(each_array);

use base qw(WTSI::NPG::Test);
use Test::More tests => 51;
use Test::Exception;

use plink_binary;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid = $$;
my $data_path = './t/qc/check/identity';
my $plink_path = "$data_path/fake_qc_genotypes";
my $plink_swap = "$data_path/fake_swap_genotypes";
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";
my $broken_snpset_file =
    "$data_path/W30467_snp_set_info_1000Genomes_BROKEN.tsv";
my $expected_json_path = "$data_path/expected_identity_results.json";
my $expected_all_json_path = "$data_path/combined_identity_expected.json";
my $expected_omit_path = "$data_path/expected_omit_results.json";
my $pass_threshold = 0.9;
my $snp_threshold = 8;

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
  my $qc_calls = _get_qc_calls();
  my $id_results = $check->find_identity($qc_calls);
  ok($id_results, "Find identity results for given QC calls");
  my @json_spec_results;
  foreach my $id_result (@{$id_results}) {
      push(@json_spec_results, $id_result->to_json_spec());
  }
  my $expected_json = decode_json(read_file($expected_json_path));
  is_deeply(\@json_spec_results, $expected_json,
            "JSON output congruent with expected values") or
              diag explain \@json_spec_results;
}

sub find_identity_insufficient_snps : Test(3) {
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($broken_snpset_file);
    my $tempdir = tempdir("IdentityTest.$pid.XXXXXX", CLEANUP => 1);
    my $json_path = "$tempdir/identity.json";
    my $csv_path = "$tempdir/identity.csv";
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_path,
         snpset     => $snpset);
    # get fake QC results for a few samples; others will appear as missing
    my $qc_calls = _get_qc_calls_broken_snpset();
    $check->write_identity_results($qc_calls, $json_path, $csv_path);
    ok(-e $json_path, "JSON identity output written");
    ok(-e $csv_path, "CSV identity output written");
    my $json_results = decode_json(read_file($json_path));
    my $expected_json = decode_json(read_file($expected_omit_path));
    is_deeply($json_results, $expected_json,
              "Results for insufficient SNPs congruent with expected values")
      or diag explain $expected_json;
}

sub num_samples : Test(1) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  cmp_ok($check->num_samples, '==', 6, 'Number of samples')
}

sub sample_names : Test(1) {
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

  my $names = $check->sample_names;
  is_deeply($names, \@expected) or diag explain $names;
}

sub shared_snp_names : Test(1) {
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

  my $shared = $check->shared_snp_names;
  is_deeply($shared, \@expected) or diag explain $shared;
}

sub production_calls : Test(18) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
  my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
    (plink_path => $plink_path,
     snpset     => $snpset);

  my $calls = $check->production_calls;

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

  my @expected_snp_names = @{$check->shared_snp_names};

  foreach my $sample_name (@{$check->sample_names}) {
    my @calls = @{$calls->{$sample_name}};

    cmp_ok(scalar @calls, '==', 20, "Number of $sample_name calls");

    my @snp_names = map { $_->snp->name } @calls;
    is_deeply(\@snp_names, \@expected_snp_names) or diag explain \@snp_names;

    my @genotypes = map { $_->genotype } @calls;
    is_deeply(\@genotypes, $expected_genotypes->{$sample_name})
      or diag explain \@genotypes;
  }
}

sub run_identity_checks : Test(3) {
    # test combined output with identity and swap checks
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $tempdir = tempdir("IdentityTest.$pid.XXXXXX", CLEANUP => 1);
    my $json_path = $tempdir."/identity.json";
    my $csv_path = $tempdir."/identity.csv";
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_path,
         snpset     => $snpset);
    # get fake QC results for a few samples; others will appear as missing
    my $qc_calls = _get_qc_calls();
    $check->write_identity_results($qc_calls, $json_path, $csv_path);
    ok(-e $json_path, "JSON identity output written");
    ok(-e $csv_path, "CSV identity output written");
    my $json_results = decode_json(read_file($json_path));
    my $json_expected = decode_json(read_file($expected_all_json_path));
    is_deeply($json_results, $json_expected,
              "Combined JSON results congruent with expected values")
      or diag explain $json_results;
}

sub sample_swap_evaluation : Test(14) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_swap,
         snpset     => $snpset);
    my $sample_ids = _get_swap_sample_identities();
    my $swap_result = $check->pairwise_swap_check($sample_ids);
    ok($swap_result, "Failed pair comparison completed");
    my @expected = (
        [
            'urn:wtsi:249442_C09_HELIC5102247',
            'urn:wtsi:249441_F11_HELIC5102138',
            1.0,
            1
        ],
        [
            'urn:wtsi:249461_G12_HELIC5215300',
            'urn:wtsi:249441_F11_HELIC5102138',
            0.9997293,
            1,
        ],
        [
            'urn:wtsi:249461_G12_HELIC5215300',
            'urn:wtsi:249442_C09_HELIC5102247',
            0.05858745,
            0
        ]
    );
    my $expected_prior = 0.666667;
    # don't use is_deeply to compare floats
    my $epsilon = 0.0005;
    my $delta = abs($swap_result->{'prior'} - $expected_prior);
    ok($delta < $epsilon, "Swap prior within tolerance");
    my $compared = $swap_result->{'comparison'};
    for (my $i=0;$i<@expected;$i++) {
        for (my $j=0;$j<4;$j++) {
            if ($j==2) {
                my $delta = abs($compared->[$i][$j] - $expected[$i][$j]);
                ok($delta < $epsilon, "Identity metric within tolerance");
            } else {
                is($compared->[$i][$j], $expected[$i][$j],
                   "Sample swap output matches expected value");
            }
        }
    }
}

sub script : Test(7) {
    # test of command-line script
    # Could move this into Scripts.pm (which is slow to run, ~10 minutes)

    my $identity_script_wip = "./bin/check_identity_bed_wip.pl";
    my $tempdir = tempdir("IdentityTest.script.$pid.XXXXXX", CLEANUP => 1);
    my $jsonPath = "$tempdir/identity.json";
    my $csvPath = "$tempdir/identity.csv";
    my $plexDir = "/nfs/srpipe_references/genotypes";
    my $plexFile = "$plexDir/W30467_snp_set_info_1000Genomes.tsv";
    my $refPath = "$data_path/identity_script_output.json";
    my $expectedCsvPath = "$data_path/identity_script_output.csv";
    my $sampleJson = "$data_path/fake_sample.json";

    ok(system(join q{ }, "$identity_script_wip",
              "--plink $data_path/fake_qc_genotypes",
              "--json $jsonPath",
              "--csv $csvPath",
              "--plex $plexFile",
              "--sample_json $sampleJson",
              "--vcf $data_path/qc_plex_calls.vcf"
          ) == 0, 'Script identity check');

    ok(-e $jsonPath, "JSON output written by script");
    ok(-e $csvPath, "CSV output written by script");
    my $outData = from_json(read_file($jsonPath));
    my $refData = from_json(read_file($refPath));
    is_deeply($outData, $refData,
              "Script JSON output matches reference file");
    my $csvGot = read_file($csvPath);
    my $csvExpected = read_file($expectedCsvPath);
    is($csvGot, $csvExpected, "Script CSV output matches reference file");

    # now test with multiple VCF files and differing SNPSets
    # VCF and manifest from above are split into two different SNP subsets
    # Expect them to produce the same result when combined
    $jsonPath = "$tempdir/identity_2.json";
    $csvPath = "$tempdir/identity_2.csv";
    my $plexFile1 = "$data_path/W30467_snp_set_info_1000Genomes_1.tsv";
    my $plexFile2 = "$data_path/W30467_snp_set_info_1000Genomes_2.tsv";
    my $vcf1 = "$data_path/qc_plex_calls_1.vcf";
    my $vcf2 = "$data_path/qc_plex_calls_2.vcf";
    ok(system(join q{ }, "$identity_script_wip",
              "--plink $data_path/fake_qc_genotypes",
              "--json $jsonPath",
              "--csv $csvPath",
              "--plex $plexFile1",
              "--plex $plexFile2",
              "--sample_json $sampleJson",
              "--vcf $vcf1",
              "--vcf $vcf2",
          ) == 0, 'Script identity check');
    $outData = from_json(read_file($jsonPath));
    is_deeply($outData, $refData,
              "Script JSON output matches reference file, 2 inputs");
}

sub _get_swap_sample_identities {

    # Some fake QC data
    # - List of 3 'failed' sample names, 2 of which are swapped
    # - Create SampleIdentityBayesian object for each sample

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $check = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new
        (plink_path => $plink_swap,
         snpset     => $snpset);
    my $qc_calls = _get_qc_calls();
    my $production_calls = $check->production_calls;

    my @sample_identities;
    my @qc_calls = _get_qc_calls();
    foreach my $sample_name (@qc_sample_names) {
        # need both QC and production calls to create object
        my %args = (sample_name      => $sample_name,
                    snpset           => $snpset,
                    production_calls => $production_calls->{$sample_name},
                    qc_calls         => $qc_calls->{$sample_name},
                    pass_threshold   => $pass_threshold);
        my $sample_id =
            WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
                  new(\%args);
        push (@sample_identities, $sample_id);
    }
    return \@sample_identities;
}

sub _get_qc_calls {

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

    my %qc_calls;
    my @callset_names = qw/callset_foo callset_bar/;
    foreach my $sample_name (@qc_sample_names) {
        my @qc_calls;
        my $i = 0;
        foreach my $input (@{$qc_data{$sample_name}}) {
            my ($snp, $genotype) = @{$input};
            my %args = (snp      => $snpset->named_snp($snp),
                        genotype => $genotype);
            if ($i < 10) { $args{'callset_name'} = $callset_names[0]; }
            else { $args{'callset_name'} = $callset_names[1]; }
            $i++;
            push @qc_calls, WTSI::NPG::Genotyping::Call->new(%args);
        }
        $qc_calls{$sample_name} = \@qc_calls;
    }
    return \%qc_calls;
}

sub _get_qc_calls_broken_snpset {
    # The 'broken' snpset tests the case of insufficient shared SNPs
    # between production and QC calls: All but two of the SNPs in the
    # standard QC plex are renamed so that they do not appear to be shared
    # with the production snpset. This method renames the snps in QC calls
    # for consistency with the 'broken' snpset.
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($broken_snpset_file);
    my %qc_calls = %{_get_qc_calls()};
    my %broken_calls = ();
    my @callset_names = qw/callset_foo callset_bar/;
    my $i = 0;
    foreach my $sample_name (keys(%qc_calls)) {
        my @calls = @{$qc_calls{$sample_name}};
        my @broken_calls = ();
        foreach my $call (@calls) {
            my $snp = $call->snp->name;
            unless ($snp eq 'rs1805087' || $snp eq 'rs2241714') {
                $snp = $snp."_BROKEN";
            }
            my %args = (snp      => $snpset->named_snp($snp),
                        genotype => $call->genotype);
            if ($i < 10) { $args{'callset_name'} = $callset_names[0]; }
            else { $args{'callset_name'} = $callset_names[1]; }
            $i++;
            push(@broken_calls, WTSI::NPG::Genotyping::Call->new(\%args));
        }
        $broken_calls{$sample_name} = \@broken_calls;
    }
    return \%broken_calls;
}

1;
