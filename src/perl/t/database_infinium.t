
# Tests WTSI::NPG::Genotyping::Database::Infinium

use utf8;

use strict;
use warnings;

use Test::More tests => 8;

BEGIN { use_ok('WTSI::NPG::Genotyping::Database::Infinium'); }
require_ok('WTSI::NPG::Genotyping::Database::Infinium');

use WTSI::NPG::Genotyping::Database::Infinium;

Log::Log4perl::init('etc/log4perl_tests.conf');

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

SKIP: {
  skip "$db_credentials credentials file not present", 6
    unless -e $db_credentials;

  my $db = WTSI::NPG::Genotyping::Database::Infinium->new
    (name   => 'infinium',
     inifile => $db_credentials);

  is($db->name, 'infinium');
  ok($db->data_source);
  ok($db->username);
  ok($db->connect(RaiseError => 1));
  ok($db->dbh);
  ok($db->disconnect);
}
