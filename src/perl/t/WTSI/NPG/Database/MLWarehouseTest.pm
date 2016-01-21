
package WTSI::NPG::Database::MLWarehouseTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Test::More tests => 13;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Database::MLWarehouse'); }

use WTSI::NPG::Database::MLWarehouse;

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

sub require : Test(1) {
  require_ok('WTSI::NPG::Database::MLWarehouse');
}

sub connect : Test(7) {
  my $db = WTSI::NPG::Database::MLWarehouse->new
    (name    => 'multi_lims_warehouse',
     inifile => $db_credentials);

  is($db->name, 'multi_lims_warehouse', 'Has correct name');
  ok($db->data_source,                  'Has a data_source');
  ok($db->username,                     'Has a username');

  ok(!$db->is_connected,            'Initially, is not connected');
  ok($db->connect(RaiseError           => 1,
                 mysql_enable_utf8     => 1,
                  mysql_auto_reconnect => 1), 'Can connect');
  ok($db->is_connected,             'Is connected');
  ok($db->schema,                   'Has a DBIC schema');
  $db->disconnect;
}

sub disconnect : Test(4) {
  my $db = WTSI::NPG::Database::MLWarehouse->new
    (name    => 'multi_lims_warehouse',
     inifile =>  $db_credentials);

  ok($db->connect(RaiseError => 1), 'Can connect');
  ok($db->is_connected,             'Is connected');
  ok($db->disconnect,               'Can disconnect');
  ok(!$db->is_connected,            'Finally, is not connected');
}

1;
