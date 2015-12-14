
use utf8;

package WTSI::NPG::Genotyping::CallTest;

use strict;
use warnings;

use Log::Log4perl;
use List::AllUtils qw(all);

use base qw(Test::Class);
use Test::Exception;
use Test::More tests => 70;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Call'); }

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNPSet;

my $data_path = './t/snpset';
my $data_file = 'qc.tsv';

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Call');
}


sub clone : Test(4) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  my $callset_name = "test_callset_name";
  my $qscore = 42;
  my $call = WTSI::NPG::Genotyping::Call->new
    (genotype     => 'TG',
     snp          => $snpset->named_snp('rs11096957'),
     callset_name => $callset_name,
     qscore       => $qscore);

  is('TG', $call->clone->genotype, 'Cloned call');
  ok($call->snp->name eq $call->clone->snp->name,
     'SNP name preserved by cloning');
  ok($call->clone->callset_name eq $callset_name,
     "Callset name preserved by cloning");
  ok($call->clone->qscore eq $qscore,
     "Qscore preserved by cloning");
}

sub constructor : Test(7) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  new_ok('WTSI::NPG::Genotyping::Call',
         [genotype => 'TG',
          snp      => $snpset->named_snp('rs11096957')]);
  new_ok('WTSI::NPG::Genotyping::Call',
         [genotype => 'GT',
          snp      => $snpset->named_snp('rs11096957')]);
  new_ok('WTSI::NPG::Genotyping::Call',
         [genotype => 'TT',
          snp      => $snpset->named_snp('rs11096957')]);
  new_ok('WTSI::NPG::Genotyping::Call',
         [genotype => 'GG',
          snp      => $snpset->named_snp('rs11096957')]);

  new_ok('WTSI::NPG::Genotyping::Call',
         [genotype => 'NN',
          snp      => $snpset->named_snp('rs11096957')]);

  dies_ok {
    WTSI::NPG::Genotyping::Call->new
        (genotype => 'CG',
         snp      => $snpset->named_snp('rs11096957'));
  } 'Cannot construct from mismatching genotype';
}

sub is_call : Test(4) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'TG',
      snp      => $snpset->named_snp('rs11096957'))->is_call,
     'Is call');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'TG',
      snp      => $snpset->named_snp('rs11096957'))->is_call,
     'Is call');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'TG',
      snp      => $snpset->named_snp('rs11096957'),
      is_call  => 0)->is_call,
     'Is no call 1');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'NN',
      snp      => $snpset->named_snp('rs11096957'))->is_call,
     'Is no call 2 (automatic))');
}

sub is_homozygous : Test(4) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'TG',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous,
     'Not homozygous 1');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'GT',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous,
     'Not homozygous 2');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'TT',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous,
     'Homozygous 1');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'GG',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous,
     'Homozygous 2');
}

sub is_heterozygous : Test(4) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'TG',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous,
     'Heterozygous 1');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'GT',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous,
     'Heterozygous 2');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'TT',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous,
     'Not heterozygous 1');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'GG',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous,
     'Not heterozygous 2');
}

sub is_homozygous_complement : Test(4) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'AC',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous_complement,
     'Not homozygous complement 1');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'CA',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous_complement,
     'Not homozygous complement 2');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'AA',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous_complement,
     'Homozygous complement 1');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'CC',
      snp      => $snpset->named_snp('rs11096957'))->is_homozygous_complement,
     'Homozygous complement 2');
}

sub is_heterozygous_complement : Test(4) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'AC',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous_complement,
     'Heterozygous complement 1');

  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'CA',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous_complement,
     'Heterozygous complement 2');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'AA',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous_complement,
     'Not heterozygous complement 1');

  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'CC',
      snp      => $snpset->named_snp('rs11096957'))->is_heterozygous_complement,
     'Not heterozygous complement 2');
}

sub is_complement : Test(8) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  # These are calls have not been reported as the complement of the
  # SNP as given in dbSNP (TG for rs11096957).
  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'TG',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is not complement 1');
  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'GT',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is not complement 2');
  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'GG',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is not complement 3');
  ok(!WTSI::NPG::Genotyping::Call->new
     (genotype => 'TT',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is not complement 4');

  # These are calls have been reported as the complement of the SNP as
  # given in dbSNP (TG for rs11096957).
  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'AC',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is complement 1');
  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'CA',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is complement 2');
  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'CC',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is complement 3');
  ok(WTSI::NPG::Genotyping::Call->new
     (genotype => 'AA',
      snp      => $snpset->named_snp('rs11096957'))->is_complement,
     'Is complement 4');
}

sub complement : Test(5) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");

  my $callset_name = "test_callset_name";
  my $qscore = 42;
  my $call = WTSI::NPG::Genotyping::Call->new
    (genotype     => 'TG',
     snp          => $snpset->named_snp('rs11096957'),
     callset_name => $callset_name,
     qscore       => $qscore);

  is('AC', $call->complement->genotype, 'Complement call');
  ok(!$call->is_complement, 'Is not complemented');
  ok($call->complement->is_complement, 'Is complemented');
  ok($call->complement->callset_name eq $callset_name,
     "Callset name preserved by complement");
  ok($call->complement->qscore eq $qscore,
     "Qscore preserved by complement");
}

sub merge : Test(9) {
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
    my $snp = $snpset->named_snp('rs11096957'); # TG
    my $other_snp = $snpset->named_snp('rs1805034'); # CT

    my $call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                genotype => 'GG',
                                                is_call  => 1);
    my $same_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                     genotype => 'GG',
                                                     is_call  =>  1);
    my $no_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                   genotype => 'NN',
                                                   is_call  =>  0);
    my $other_no_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                         genotype => 'NN',
                                                         is_call  =>  0);
    my $conflicting_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                            genotype => 'TT',
                                                            is_call  => 1);
    my $other_snp_call = WTSI::NPG::Genotyping::Call->new
        (snp      => $other_snp,
         genotype => 'CC',
         is_call  => 1);

    is($call->merge($same_call)->genotype, 'GG', 'Merge of identical calls');

    # test merge of call and no-call (or vice versa)
    my $merged = $call->merge($no_call);
    is($merged->genotype, 'GG',
       'Merge of call and no-call has correct genotype');
    ok($merged->is_call, 'Merge of call and no-call has is_call true');

    $merged = $no_call->merge($call);
    is($merged->genotype, 'GG',
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

sub equivalent : Test(15) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
  my $snp = $snpset->named_snp('rs11096957'); # TG
  my $other_snp = $snpset->named_snp('rs1805034'); # CT

  my $call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                              genotype => 'TG',
                                              is_call  => 1);

  my $same_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                   genotype => 'TG',
                                                   is_call  =>  1);
  ok($same_call->is_heterozygous);
  ok($call->equivalent($same_call), 'Exact equivalent');
  ok($same_call->equivalent($call), 'Exact equivalent (reciprocal)');

  my $comp_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                   genotype => 'AC',
                                                   is_call  =>  1);
  ok($comp_call->is_heterozygous_complement);
  ok($call->equivalent($comp_call), 'Complement equivalent');
  ok($comp_call->equivalent($call), 'Complement equivalent (reciprocal)');

  my $rev_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                  genotype => 'GT',
                                                  is_call  =>  1);
  ok($rev_call->is_heterozygous);
  ok($call->equivalent($rev_call), 'Reverse equivalent');
  ok($rev_call->equivalent($call), 'Reverse equivalent (reciprocal)');

  my $revcomp_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                      genotype => 'CA',
                                                      is_call  =>  1);
  ok($revcomp_call->is_heterozygous_complement);
  ok($call->equivalent($revcomp_call), 'Reverse complement equivalent');
  ok($revcomp_call->equivalent($call),
     'Reverse complement equivalent (reciprocal)');

  my $no_call = WTSI::NPG::Genotyping::Call->new(snp      => $snp,
                                                genotype => 'TG',
                                                is_call  => 0);
  ok(!$call->equivalent($no_call), 'No call not equivalent');
  ok(!$no_call->equivalent($call), 'No call not equivalent (reciprocal)');
  ok(!$no_call->equivalent($no_call), 'No call not equivalent with self');
}

1;

