
use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayResultTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Log::Log4perl;
use Test::More tests => 6;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::AssayResult'); }

use WTSI::NPG::Genotyping::Sequenom::AssayResult;

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::AssayResult');
}

sub constructor : Test(1) {
  new_ok('WTSI::NPG::Genotyping::Sequenom::AssayResult',
         [allele        => 'C',
          assay_id      => 'assay1-rs012345678',
          chip          => '1234',
          customer      => 'customer1',
          experiment    => 'experiment1',
          genotype_id   => 'CT',
          height        => 10,
          mass          => 1,
          plate         => 'plate1',
          project       => 'project1',
          sample_id     => 'sample1',
          status        => 'status1',
          well_position => 'A01',
          str           =>  join("\t", 'C', 'assay1-rs012345678', '1234',
                                 'customer1', 'experiment1', 'CT', 10, 1,
                                 'plate1', 'project1', 'sample1', 'status1',
                                 'A01')]);
}

sub snpset_name : Test(1) {
  my $result = WTSI::NPG::Genotyping::Sequenom::AssayResult->new
    (allele        => 'C',
     assay_id      => 'assay1-rs012345678',
     chip          => '1234',
     customer      => 'customer1',
     experiment    => 'experiment1',
     genotype_id   => 'CT',
     height        => 10,
     mass          => 1,
     plate         => 'plate1',
     project       => 'project1',
     sample_id     => 'sample1',
     status        => 'status1',
     well_position => 'A01',
     str           => '');

  is($result->snpset_name, 'assay1', 'SNP set name');
}

sub snp_assayed : Test(1) {
  my $result = WTSI::NPG::Genotyping::Sequenom::AssayResult->new
    (allele        => 'C',
     assay_id      => 'assay1-rs012345678',
     chip          => '1234',
     customer      => 'customer1',
     experiment    => 'experiment1',
     genotype_id   => 'CT',
     height        => 10,
     mass          => 1,
     plate         => 'plate1',
     project       => 'project1',
     sample_id     => 'sample1',
     status        => 'status1',
     well_position => 'A01',
     str           => '');

  is($result->snp_assayed, 'rs012345678', 'SNP assayed');
}

sub canonical_call : Test(1) {
  

  foreach my $call ('AA', 'NN', '') {
    
  }
}

1;
