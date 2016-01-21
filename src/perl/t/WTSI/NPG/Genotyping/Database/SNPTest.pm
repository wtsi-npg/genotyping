
use utf8;

package WTSI::NPG::Genotyping::Database::SNPTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Test::More tests => 13;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Database::SNP'); }

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Database::SNP');
}

sub connect : Test(7) {
 SKIP: {
    skip "$db_credentials credentials file not present", 7
      unless -e $db_credentials;

    my $db = WTSI::NPG::Genotyping::Database::SNP->new
      (name    => 'snp',
       inifile => $db_credentials);

    is($db->name, 'snp', 'Has correct name');
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

    my $db = WTSI::NPG::Genotyping::Database::SNP->new
      (name    => 'snp',
       inifile => $db_credentials);

    ok($db->connect(RaiseError => 1), 'Can connect');
    ok($db->is_connected, 'Is connected');
    ok($db->disconnect, 'Can disconnect');
    ok(!$db->is_connected, 'Finally, is not connected');
  }
}

1;
