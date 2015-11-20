use strict;
use warnings;

use Log::Log4perl;
use List::AllUtils qw(all);

use base qw(Test::Class);
use Test::Exception;
use Test::More tests => 27;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::GenderMarkerCall'); }

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::GenderMarkerCall;
use WTSI::NPG::Genotyping::SNPSet;

my $data_path = './t/snpset';
my $data_file = 'qc.tsv';

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::GenderMarkerCall');
}

sub constructor : Test(7) {
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
  my $snp = $snpset->named_snp('GS34251');
  new_ok('WTSI::NPG::Genotyping::GenderMarkerCall',
         [genotype => 'TT',
          snp      => $snp]);
  new_ok('WTSI::NPG::Genotyping::GenderMarkerCall',
         [genotype => 'CC',
          snp      => $snp]);
  new_ok('WTSI::NPG::Genotyping::GenderMarkerCall',
         [genotype => 'NN',
          snp      => $snp]);
  new_ok('WTSI::NPG::Genotyping::GenderMarkerCall',
         [genotype => 'CT',
          snp      => $snp]); # can have a het call for a male sample
  my $x_call = WTSI::NPG::Genotyping::Call->new(
      snp      => $snp->x_marker,
      genotype => 'TT'
  );
  my $y_call = WTSI::NPG::Genotyping::Call->new(
      snp      => $snp->y_marker,
      genotype => 'CC'
  );
  # alternate constructor arguments
  new_ok('WTSI::NPG::Genotyping::GenderMarkerCall',
         [x_call => $x_call,
          y_call => $y_call]);
  my $y_no_call = WTSI::NPG::Genotyping::Call->new(
      snp      => $snp->y_marker,
      genotype => 'NN',
      is_call  => 0
  );
  new_ok('WTSI::NPG::Genotyping::GenderMarkerCall',
         [x_call => $x_call,
          y_call => $y_no_call]);
  dies_ok{
      WTSI::NPG::Genotyping::GenderMarkerCall->new(
          genotype => 'TT',
          snp      => $snp,
          x_call   => $x_call,
          y_call   => $y_call
      );

  } "Dies with too many arguments to constructor";

}

sub equivalent : Test(8) {
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
    my $snp = $snpset->named_snp('GS34251'); # TT/CC
    my $non_gendermarker_snp = $snpset->named_snp('rs11096957'); # TG
    my $call = WTSI::NPG::Genotyping::GenderMarkerCall->new
        (snp      => $snp,
         genotype => 'TT',
         is_call  => 1);
    my $same_call = WTSI::NPG::Genotyping::GenderMarkerCall->new
        (snp      => $snp,
         genotype => 'TT',
         is_call  =>  1);
    ok($call->equivalent($same_call), 'Exact equivalent');
    ok($same_call->equivalent($call), 'Exact equivalent (reciprocal)');
    ok($call->equivalent($call->complement()), 'Complement equivalent');
    ok($call->complement->equivalent($call),
       'Complement equivalent (reciprocal)');
    my $non_gendermarker_call = WTSI::NPG::Genotyping::Call->new
        (snp      => $non_gendermarker_snp,
         genotype => 'TT',
         is_call  =>  1);
    dies_ok{
        $call->equivalent($non_gendermarker_call);
    } "Dies on equivalence check with non-gendermarker call";
    my $no_call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
        snp      => $snp,
        genotype => 'TT',
        is_call  => 0);
    ok(!$call->equivalent($no_call), 'No call not equivalent');
    ok(!$no_call->equivalent($call), 'No call not equivalent (reciprocal)');
    ok(!$no_call->equivalent($no_call), 'No call not equivalent with self');
}

sub gender_attribute : Test(6) {
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
    my $snp = $snpset->named_snp('GS34251');
    my $x_call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
        genotype => 'TT',
        snp      => $snp
    );
    ok($x_call->is_female(), "Female gender marker call is an X call");
    ok(!$x_call->is_male(), "Female gender marker call is not a Y call");
    my $y_call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
        genotype => 'CT',
        snp      => $snp
    );
    ok(!$y_call->is_female(), "Male gender marker call is not an X call");
    ok($y_call->is_male(), "Male gender marker call is a Y call");
    my $no_call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
        genotype => 'NN',
        snp      => $snp,
        is_call  => 0
    );
    ok(!$no_call->is_female(), "Null gender marker call is not an X call");
    ok(!$no_call->is_male(), "Null gender marker call is not a Y call");
}

sub complement : Test(4) {
   my $snpset = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
   my $snp = $snpset->named_snp('GS34251');
   my $x_call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
       genotype => 'TT',
       snp      => $snp
   );
   isa_ok($x_call->complement, 'WTSI::NPG::Genotyping::GenderMarkerCall',
      "Complement of a GenderMarkerCall is another GenderMarkerCall");
   is('AA', $x_call->complement->genotype, 'Complement call');
   ok(!$x_call->is_complement, 'Is not complemented');
   ok($x_call->complement->is_complement, 'Is complemented');
}
