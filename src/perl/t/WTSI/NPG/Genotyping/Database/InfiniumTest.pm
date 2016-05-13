
use utf8;

package WTSI::NPG::Genotyping::Database::InfiniumTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Test::More tests => 14;
use Test::Exception;
use Test::MockObject;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Database::Infinium'); }

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Database::Infinium');
}

sub connect : Test(7) {
 SKIP: {
    skip "$db_credentials credentials file not present", 7
      unless -e $db_credentials;

    my $db = WTSI::NPG::Genotyping::Database::Infinium->new
      (name    => 'infinium',
       inifile => $db_credentials);

    is($db->name, 'infinium', 'Has correct name');
    ok($db->data_source, 'Has a data_source');
    ok($db->username, 'Has a username');

    ok(!$db->is_connected, 'Initially, is not connected');
    ok($db->connect(RaiseError => 1), 'Can connect');
    ok($db->is_connected, 'Is connected');
    ok($db->dbh, 'Has a database handle');

    $db->disconnect;
  }
}

sub disconnect : Test(4) {
 SKIP: {
    skip "$db_credentials credentials file not present", 4
      unless -e $db_credentials;

    my $db = WTSI::NPG::Genotyping::Database::Infinium->new
      (name    => 'infinium',
       inifile => $db_credentials);

    ok($db->connect(RaiseError => 1), 'Can connect');
    ok($db->is_connected, 'Is connected');
    ok($db->disconnect, 'Can disconnect');
    ok(!$db->is_connected, 'Finally, is not connected');
  }
}

sub repeat_scans : Test(1) {

  my $db = WTSI::NPG::Genotyping::Database::Infinium->new
    (name    => 'infinium',
     inifile => $db_credentials);

  # This test is fragile because it relies on knowing about the
  # internals of the package under test. However, it's better than
  # nothing, until we commit resources to refactoring.
  my $sth = Test::MockObject->new;
  $sth->set_true('execute');
  $sth->set_series('fetchrow_hashref',
                   {plate             => 'ABC123456-DNA',
                    well              => 'A01',
                    sample            => 'sample1',
                    beadchip          => '0123456789',
                    beadchip_section  => 'R01C01',
                    beadchip_design   => 'test_design',
                    beadchip_revision => '1',
                    status            => 'Pass',
                    gtc_file          => 'test.gtc',
                    idat_grn_file     => 'test_Grn.idat',
                    idat_red_file     => 'test_Red.idat',
                    idat_grn_path     => '/test',
                    idat_red_path     => '/test',
                    image_date        => '',
                    image_iso_date    => '2016-10-01'},
                   {plate             => 'ABC123456-DNA',
                    well              => 'A01',
                    sample            => 'sample1',
                    beadchip          => '0123456789',
                    beadchip_section  => 'R01C01',
                    beadchip_design   => 'test_design',
                    beadchip_revision => '1',
                    status            => 'Pass',
                    gtc_file          => 'test.gtc',
                    idat_grn_file     => 'test_Grn.idat',
                    idat_red_file     => 'test_Red.idat',
                    idat_grn_path     => '/test',
                    idat_red_path     => '/test',
                    image_date        => '',
                    image_iso_date    => '2016-10-02'},
                   {plate             => 'ABC123456-DNA',
                    well              => 'A01',
                    sample            => 'sample1',
                    beadchip          => '0123456789',
                    beadchip_section  => 'R01C01',
                    beadchip_design   => 'test_design',
                    beadchip_revision => '1',
                    status            => 'Pass',
                    gtc_file          => 'test.gtc',
                    idat_grn_file     => 'test_Grn.idat',
                    idat_red_file     => 'test_Red.idat',
                    idat_grn_path     => '/test',
                    idat_red_path     => '/test',
                    image_date        => '',
                    image_iso_date    => '2016-10-03'});

  my $dbh = Test::MockObject->new;
  $dbh->set_always('prepare', $sth);
  $db->dbh($dbh);

  is_deeply($db->find_project_samples('test'),
            [{plate             => 'ABC123456-DNA',
              well              => 'A01',
              sample            => 'sample1',
              beadchip          => '0123456789',
              beadchip_section  => 'R01C01',
              beadchip_design   => 'test_design',
              beadchip_revision => '1',
              status            => 'Pass',
              gtc_file          => 'test.gtc',
              idat_grn_file     => 'test_Grn.idat',
              idat_red_file     => 'test_Red.idat',
              idat_grn_path     => '/test',
               idat_red_path     => '/test',
              image_date        => '',
              image_iso_date    => '2016-10-03'}],
             'Repeat scans resolved to latest');
}
1;
