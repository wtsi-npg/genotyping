
use utf8;

package WTSI::NPG::Genotyping::CallTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 11;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Call') };

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;

my ($snp, $other_snp);

sub call_fixture : Test(setup) {

    $snp = WTSI::NPG::Genotyping::SNP->new(name       => 'rs123456',
                                           ref_allele => 'A',
                                           alt_allele => 'T',
                                           chromosome => '1',
                                           position   => 1000000,
                                           strand     => '+',
                                           str        => 'dummy_1');


    $other_snp = WTSI::NPG::Genotyping::SNP->new(name       => 'rs12345678',
                                                 ref_allele => 'C',
                                                 alt_allele => 'G',
                                                 chromosome => '1',
                                                 position   => 2000000,
                                                 strand     => '+',
                                                 str        => 'dummy_2');
}


sub constructor : Test(1) {
    new_ok('WTSI::NPG::Genotyping::Call',
           [snp      => $snp,
            genotype => 'AA',
            is_call  => 1]);
}

sub merge : Test(9) {

    my $call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                genotype => 'AA',
                                                is_call  => 1);
    my $same_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                     genotype => 'AA',
                                                     is_call  =>  1);
    my $no_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                   genotype => 'NN',
                                                   is_call  =>  0);
    my $other_no_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                         genotype => 'NN',
                                                         is_call  =>  0);
    my $conflicting_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                            genotype => 'AT',
                                                            is_call  => 1);
    my $other_snp_call = WTSI::NPG::Genotyping::Call->new
        (snp      => $other_snp,
         genotype => 'GG',
         is_call  => 1);

    is($call->merge($same_call)->genotype, 'AA', 'Merge of identical calls');

    # test merge of call and no-call (or vice versa)
    my $merged = $call->merge($no_call);
    is($merged->genotype, 'AA',
       'Merge of call and no-call has correct genotype');
    ok($merged->is_call, 'Merge of call and no-call has is_call true');
    $merged = $no_call->merge($call);
    is($merged->genotype, 'AA',
       'Merge of no-call and call has correct genotype');
    ok($merged->is_call, 'Merge of no-call and call has is_call true');
    $merged = $no_call->merge($other_no_call);
    is($merged->genotype, 'NN',
       'Merge of no-calls has correct genotype');
    ok(!($merged->is_call), 'Merge of no-calls has is_call false');

    # test conflicting merge
    dies_ok(sub { $call->merge($conflicting_call) }, 'Dies on merge conflict');

    # test conflicting snps
    dies_ok(sub {$call->merge($other_snp_call)}, 'Dies on non-equal SNPs' );
}

1;
