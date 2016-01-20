
package WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesianTest;

use strict;
use warnings;
use File::Slurp qw(read_file);
use JSON;

use base qw(Test::Class);
use Test::More tests => 9;
use Test::Exception;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = './t/qc/check/identity';
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";
my $sample_name  = 'urn:wtsi:249442_C09_HELIC5102247';
my $pass_threshold = 0.9;
my ($qc_calls, $production_calls, $qc_calls_small, $production_calls_small);

our @CALLSET_NAMES = qw/callset_bar callset_foo/;

sub setup : Test(setup) {

    # copy-pasted from SampleIdentityTest.pm
    # TODO factor out shared setup code?

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

    my @qc_calls;
    my $i = 0;
    foreach my $input (@qc_data) {
        my ($snp, $genotype) = @{$input};
        my $callset_name;
        if ($i<10) { $callset_name = $CALLSET_NAMES[0]; }
        else { $callset_name = $CALLSET_NAMES[1]; }
        $i++;
        my $call = WTSI::NPG::Genotyping::Call->new
            (snp          => $snpset->named_snp($snp),
             genotype     => $genotype,
             callset_name => $callset_name
         );
        push @qc_calls, $call;
    }
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

    ##############################################################
    # smaller 'toy' dataset for development

    my @qc_data_small = (['rs649058',   'GA'], # AG (sample data)
                         ['rs1131498',  'AA'],
                         ['rs1805087',  'AA'], # AG x
                         ['rs3795677',  'TT'], # AG x
                         ['rs6166',     'GA'], # AG
                         ['rs1801262',  'AA'],
                     );


    my @qc_calls_small = map {
        my ($snp, $genotype) = @$_;

        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @qc_data_small;

    $qc_calls_small = \@qc_calls_small;

    my @production_data_small = ( ['rs649058',   'AG'],
                                  ['rs1131498',  'AA'],
                                  ['rs1805087',  'AG'],
                                  ['rs3795677',  'AG'],
                                  ['rs6166',     'GA'],
                                  ['rs1801262',  'AA'],
                              );

    my @production_calls_small = map {
        my ($snp, $genotype) = @$_;

        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @production_data_small;

    $production_calls_small = \@production_calls_small;

}


sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian');
}

sub construct : Test(1) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my @args = (sample_name      => $sample_name,
                snpset           => $snpset,
                production_calls => $production_calls,
                qc_calls         => $qc_calls,
                pass_threshold   => $pass_threshold);

    new_ok('WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian'
               => \@args);
}

sub output : Test(5) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my $sib = WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
        new(sample_name      => $sample_name,
            snpset           => $snpset,
            production_calls => $production_calls,
            qc_calls         => $qc_calls,
            pass_threshold   => $pass_threshold);
    is_deeply($sib->qc_callset_names, \@CALLSET_NAMES,
              "QC callset names match");

    my $expected_csv = 'urn:wtsi:249442_C09_HELIC5102247,assayed,0.999989,'.
        '0.8667,30,30,26,0.8000,10,10,8,0.9000,20,20,18';
    is($sib->to_csv(), $expected_csv, "CSV string matches expected value");

    # change order of callset names, and add an unknown dummy callset
    my @alternate_callset_names = ($CALLSET_NAMES[1],
                                   $CALLSET_NAMES[0],
                                   'callset_null');

    my $alternate_csv = 'urn:wtsi:249442_C09_HELIC5102247,assayed,0.999989,'.
        '0.8667,30,30,26,0.9000,20,20,18,0.8000,10,10,8,0.0000,0,0,0';
    is($sib->to_csv(\@alternate_callset_names),
       $alternate_csv, "Alternate CSV string matches expected value");

    # remove (some) callset names and check CSV output
    my @anonymous_qc_calls;
    foreach my $call (@{$qc_calls}) {
        my $new_call;
        if ($call->callset_name eq $CALLSET_NAMES[0]) {
            $new_call = $call;
        } else {
            # omit callset_name argument to Call constructor
            $new_call = WTSI::NPG::Genotyping::Call->new
                (snp          => $call->snp,
                 genotype     => $call->genotype);
        }
        push @anonymous_qc_calls, $new_call;
    }
    my $anon_sib =
        WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
              new(sample_name      => $sample_name,
                  snpset           => $snpset,
                  production_calls => $production_calls,
                  qc_calls         => \@anonymous_qc_calls,
                  pass_threshold   => $pass_threshold);
    is_deeply($anon_sib->qc_callset_names,
              ['_unknown_callset_', 'callset_bar'],
              "Anonymous calls assigned to 'unknown' callset name");
    # order of call subsets is transposed
    my $anon_csv = 'urn:wtsi:249442_C09_HELIC5102247,assayed,0.999989,'.
        '0.8667,30,30,26,0.9000,20,20,18,0.8000,10,10,8';
    is($anon_sib->to_csv(), $anon_csv, "CSV string matches expected value");
}


sub metric : Test(2) {

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);
    my $delta = 0.000001;
    my $expected_big = 0.999989;
    my $expected_small = 0.497580;

    my %args = (sample_name      => $sample_name,
                snpset           => $snpset,
                production_calls => $production_calls,
                qc_calls         => $qc_calls,
                pass_threshold   => $pass_threshold);
    my $sib = WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
              new(\%args);
    ok(abs($sib->identity - $expected_big) < $delta,
       "Identity matches expected value, large test set");

    %args = (sample_name      => $sample_name,
                snpset           => $snpset,
                production_calls => $production_calls_small,
                qc_calls         => $qc_calls_small,
                pass_threshold   => $pass_threshold);
    my $sib_small =
        WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian->
              new(\%args);
    ok(abs($sib_small->identity - $expected_small) < $delta,
       "Identity matches expected value, small test set");

}


1;
