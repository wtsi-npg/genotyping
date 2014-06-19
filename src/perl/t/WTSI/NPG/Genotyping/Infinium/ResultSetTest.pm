
use utf8;

package WTSI::NPG::Genotyping::Infinium::ResultSetTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 8;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Infinium::ResultSet'); }

use WTSI::NPG::Genotyping::Infinium::ResultSet;

my $data_path = './t/infinium_resultset';
my $gtc_path = "$data_path/0123456789_R01C01.gtc";
my $grn_path = "$data_path/0123456789_R01C01_Grn.idat";
my $red_path = "$data_path/0123456789_R01C01_Red.idat";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Infinium::ResultSet');
}

sub constructor : Test(6) {
  new_ok('WTSI::NPG::Genotyping::Infinium::ResultSet',
         [beadchip         => '0123456789',
          beadchip_section => 'R01C01',
          gtc_file         => $gtc_path,
          grn_idat_file    => $grn_path,
          red_idat_file    => $red_path]);

  new_ok('WTSI::NPG::Genotyping::Infinium::ResultSet',
         [beadchip         => '0123456789',
          beadchip_section => 'R01C01',
          is_methylation   => 1,
          grn_idat_file    => $grn_path,
          red_idat_file    => $red_path]);

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (beadchip         => '0123456789',
         beadchip_section => 'R01C01',
         gtc_file         => 'no_such_path',
         grn_idat_file    => $grn_path,
         red_idat_file    => $red_path)
      }
    "Expected to fail when the GTC file does not exist";

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (beadchip         => '0123456789',
         beadchip_section => 'R01C01',
         is_methylation   => 1,
         gtc_file         => $gtc_path,
         grn_idat_file    => $grn_path,
         red_idat_file    => $red_path)
      }
    "Expected to fail when a GTC file is provided for a methylation result";

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (beadchip         => '0123456789',
         beadchip_section => 'R01C01',
         gtc_file         => $gtc_path,
         grn_idat_file    => 'no_such_path',
         red_idat_file    => $red_path)
      }
    "Expected to fail when the Grn idat file does not exist";

  dies_ok {
    WTSI::NPG::Genotyping::Infinium::ResultSet->new
        (beadchip         => '0123456789',
         beadchip_section => 'R01C01',
         gtc_file         => $gtc_path,
         grn_idat_file    => $grn_path,
         red_idat_file    => 'no_such_path')
      }
    "Expected to fail when the Red idat file does not exist";
}
