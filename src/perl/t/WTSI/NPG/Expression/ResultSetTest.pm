package WTSI::NPG::Expression::ResultSetTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Test::More tests => 5;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Expression::ResultSet'); }

use WTSI::NPG::Expression::ResultSet;

my $data_path = './t/expression_resultset';
my $idat_path = "$data_path/012345678901_A_Grn.idat";
my $xml_path = "$data_path/012345678901_A_Grn.xml";

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::ResultSet');
}

sub constructor : Test(3) {
  new_ok('WTSI::NPG::Expression::ResultSet',
         [sample_id        => 'sample1',
          beadchip         => '012345678901',
          beadchip_section => 'A',
          idat_file        => $idat_path,
          xml_file         => $xml_path]);

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (sample_id        => 'sample1',
         beadchip         => '012345678901',
         beadchip_section => 'A',
         idat_file        => 'no_such_path',
         xml_file         => $xml_path);
  }
    "Expected to fail when the idat file does not exist";

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (sample_id        => 'sample1',
         beadchip         => '012345678901',
         beadchip_section => 'A',
         idat_file        => $idat_path,
         xml_file         => 'no_such_path');
  }
    "Expected to fail when the xml file does not exist";
}

1;
