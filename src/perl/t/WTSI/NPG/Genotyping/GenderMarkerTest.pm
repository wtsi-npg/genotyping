
use utf8;

package WTSI::NPG::Genotyping::GenderMarkerTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 17;
use Test::Exception;

use File::Temp qw(tempfile);
use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::GenderMarker') };

use WTSI::NPG::Genotyping::GenderMarker;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::GenderMarker');
}

sub constructor : Test(6) {
  my $fh = File::Temp->new;
  close $fh;
  my $file_name = $fh->filename;
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new
    (file_name => $file_name);

  my $x1 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS34251',
     ref_allele => 'T',
     alt_allele => 'T',
     chromosome => 'X',
     position   => 88666325,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS34251	T	T	X	88666325	+');
  my $y1 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS34251',
     ref_allele => 'C',
     alt_allele => 'C',
     chromosome => 'Y',
     position   => 2934912,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS34251	C	C	Y	2934912	+');

  my $x2 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS35220',
     ref_allele => 'C',
     alt_allele => 'C',
     chromosome => 'X',
     position   => 90473610,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS35220	C	C	X	90473610	+');
  my $y2 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS35220',
     ref_allele => 'T',
     alt_allele => 'T',
     chromosome => 'Y',
     position   => 4550107,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS35220	T	T	Y	4550107	+');

  new_ok('WTSI::NPG::Genotyping::GenderMarker',
         [name     => 'GS34251',
          x_marker => $x1,
          y_marker => $y1]);

  new_ok('WTSI::NPG::Genotyping::GenderMarker',
         [name     => 'GS35220',
          x_marker => $x2,
          y_marker => $y2]);

  dies_ok {
    ok(WTSI::NPG::Genotyping::GenderMarker->new
       (name     => 'GS34251',
        x_marker => $x1,
        y_marker => $x1));
  } 'Cannot construct with X marker on Y chromosome';

  dies_ok {
    WTSI::NPG::Genotyping::GenderMarker->new
        (name     => 'GS34251',
         x_marker => $y1,
         y_marker => $y1);
  } 'Cannot construct with Y marker on X chromosome';

  my $other_snpset = WTSI::NPG::Genotyping::SNPSet->new
    (file_name => $file_name);

  my $x3 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS35220',
     ref_allele => 'C',
     alt_allele => 'C',
     chromosome => 'X',
     position   => 90473610,
     snpset     => $other_snpset,
     strand     => '+',
     str        => 'GS35220	C	C	X	90473610	+');
  my $y3 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS35220',
     ref_allele => 'T',
     alt_allele => 'T',
     chromosome => 'Y',
     position   => 4550107,
     snpset     => $other_snpset,
     strand     => '+',
     str        => 'GS35220	T	T	Y	4550107	+');

  dies_ok {
    WTSI::NPG::Genotyping::GenderMarker->new
        (name     => 'GS34251',
         x_marker => $x1,
         y_marker => $y3);
  } 'Cannot construct with markers for different SNPSet refs 1';

  dies_ok {
    WTSI::NPG::Genotyping::GenderMarker->new
        (name     => 'GS34251',
         x_marker => $x3,
         y_marker => $y1);
  } 'Cannot construct with markers for different SNPSet refs 2';
}

sub degelation : Test(5) {
  my $fh = File::Temp->new;
  close $fh;
  my $file_name = $fh->filename;
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new
    (file_name => $file_name);

  my $x = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS34251',
     ref_allele => 'T',
     alt_allele => 'T',
     chromosome => 'X',
     position   => 88666325,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS34251	T	T	X	88666325	+');
  my $y = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS34251',
     ref_allele => 'C',
     alt_allele => 'C',
     chromosome => 'Y',
     position   => 2934912,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS34251	C	C	Y	2934912	+');

  my $gm = WTSI::NPG::Genotyping::GenderMarker->new
    (name     => 'GS34251',
     x_marker => $x,
     y_marker => $y);

  is($gm->chromosome, $x->chromosome, "delegate chromsome");
  is($gm->strand,     $x->strand,     "delegate strand");
  is($gm->ref_allele, $x->ref_allele, "delegate ref_allele");
  is($gm->alt_allele, $y->alt_allele, "delegate alt_allele");

  cmp_ok($gm->position, '==', $x->position, "delegate position");
}

sub equals : Test(4) {
  my $fh = File::Temp->new;
  close $fh;
  my $file_name = $fh->filename;
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new
    (file_name => $file_name);

  my $x1 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS34251',
     ref_allele => 'T',
     alt_allele => 'T',
     chromosome => 'X',
     position   => 88666325,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS34251	T	T	X	88666325	+');
  my $y1 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS34251',
     ref_allele => 'C',
     alt_allele => 'C',
     chromosome => 'Y',
     position   => 2934912,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS34251	C	C	Y	2934912	+');

  my $gm1a = WTSI::NPG::Genotyping::GenderMarker->new
    (name     => 'GS34251',
     x_marker => $x1,
     y_marker => $y1);
  my $gm1b =  WTSI::NPG::Genotyping::GenderMarker->new
    (name     => 'GS34251',
     x_marker => $x1,
     y_marker => $y1);

  ok($gm1a->equals($gm1b), "a equals b");
  ok($gm1b->equals($gm1a), "b equals a");

  my $x2 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS35220',
     ref_allele => 'C',
     alt_allele => 'C',
     chromosome => 'X',
     position   => 90473610,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS35220	C	C	X	90473610	+');
  my $y2 = WTSI::NPG::Genotyping::SNP->new
    (name       => 'GS35220',
     ref_allele => 'T',
     alt_allele => 'T',
     chromosome => 'Y',
     position   => 4550107,
     snpset     => $snpset,
     strand     => '+',
     str        => 'GS35220	T	T	Y	4550107	+');

  my $gm2 = WTSI::NPG::Genotyping::GenderMarker->new
    (name     => 'GS34251',
     x_marker => $x2,
     y_marker => $y2);

  ok(!$gm1a->equals($gm2), "a !equals b");
  ok(!$gm2->equals($gm1a), "b !equals b");
}

1;
