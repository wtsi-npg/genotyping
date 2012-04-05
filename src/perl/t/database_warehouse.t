
# Tests WTSI::Genotyping::Database::Warehouse

use utf8;

use strict;
use warnings;

use Test::More tests => 8;

BEGIN { use_ok('WTSI::Genotyping::Database::Warehouse'); }
require_ok('WTSI::Genotyping::Database::Warehouse');

use WTSI::Genotyping::Database::Warehouse;

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

SKIP: {
  skip "$db_credentials credentials file not present", 6
    unless -e $db_credentials;

  my $db = WTSI::Genotyping::Database::Warehouse->new
    (name   => 'sequencescape_warehouse',
     inifile =>  $db_credentials);

  is($db->name, 'sequencescape_warehouse');
  ok($db->data_source);
  ok($db->username);
  ok($db->connect(RaiseError => 1));
  ok($db->dbh);
  ok($db->disconnect);
}
