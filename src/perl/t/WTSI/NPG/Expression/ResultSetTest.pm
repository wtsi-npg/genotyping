package WTSI::NPG::Expression::ResultSetTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::Expression::ResultSet;

my $data_path = './t/expression_resultset';
my $idat_path = "$data_path/012345678901_A_Grn.idat";
my $xml_path = "$data_path/012345678901_A_Grn.xml";

sub require : Test(1) {
  require_ok('WTSI::NPG::Expression::ResultSet');
}

sub constructor : Test(5) {
  new_ok('WTSI::NPG::Expression::ResultSet',
         [sample_id        => 'sample1',
          plate_id         => '123456',
          well_id          => 'A1',
          beadchip         => '012345678901',
          beadchip_section => 'A',
          idat_file        => $idat_path,
          xml_file         => $xml_path]);

  dies_ok {
    WTSI::NPG::Expression::ResultSet->new
        (sample_id        => 'sample1',
         well_id          => 'A1',
         beadchip         => '012345678901',
         beadchip_section => 'A',
         idat_file        => $idat_path,
         xml_file         => $xml_path);
  } "Expected to fail when a plate_id is not supplied";

  dies_ok {
    WTSI::NPG::Expression::ResultSet->new
        (sample_id        => 'sample1',
         plate_id         => '123456',
         beadchip         => '012345678901',
         beadchip_section => 'A',
         idat_file        => $idat_path,
         xml_file         => $xml_path);
  } "Expected to fail when a well_id is not supplied";

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (sample_id        => 'sample1',
         beadchip         => '012345678901',
         beadchip_section => 'A',
         idat_file        => 'no_such_path',
         xml_file         => $xml_path);
  } "Expected to fail when the idat file does not exist";

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (sample_id        => 'sample1',
         beadchip         => '012345678901',
         beadchip_section => 'A',
         idat_file        => $idat_path,
         xml_file         => 'no_such_path');
  } "Expected to fail when the xml file does not exist";
}

1;
