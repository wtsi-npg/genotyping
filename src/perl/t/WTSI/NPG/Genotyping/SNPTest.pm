
use utf8;

package WTSI::NPG::Genotyping::SNPTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 5;
use Test::Exception;
use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::SNP') };

use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::SNP');
}

sub constructor : Test(1) {
    new_ok('WTSI::NPG::Genotyping::SNP',
           [name       => 'rs123456',
            ref_allele => 'A',
            alt_allele => 'T',
            chromosome => '1',
            position   => 1000000,
            strand     => '+',
            str        => 'dummy_1']);
}

sub equals : Test(2) {
    my $snp = WTSI::NPG::Genotyping::SNP->new
        (name       => 'rs123456',
         ref_allele => 'A',
         alt_allele => 'T',
         chromosome => '1',
         position   => 1000000,
         strand     => '+',
         str        => 'dummy_1');

    # note that the 'str' field differs
    my $equivalent_snp = WTSI::NPG::Genotyping::SNP->new
        (name       => 'rs123456',
         ref_allele => 'A',
         alt_allele => 'T',
         chromosome => '1',
         position   => 1000000,
         strand     => '+',
         str        => 'same_dummy_different_raw_input');

    my $other_snp = WTSI::NPG::Genotyping::SNP->new
        (name       => 'rs12345678',
         ref_allele => 'C',
         alt_allele => 'G',
         chromosome => '1',
         position   => 2000000,
         strand     => '+',
         str        => 'dummy_2');

    ok($snp->equals($equivalent_snp), "Equivalent SNPs are equal");
    ok(!($snp->equals($other_snp)), "Differing SNPs are not equal");
}

1;
