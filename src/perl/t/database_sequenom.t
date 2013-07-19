
# Tests WTSI::NPG::Genotyping::Database::Sequenom

use utf8;

use strict;
use warnings;

use Test::More tests => 8;

BEGIN { use_ok('WTSI::NPG::Genotyping::Database::Sequenom'); }
require_ok('WTSI::NPG::Genotyping::Database::Sequenom');

use WTSI::NPG::Genotyping::Database::Sequenom;

Log::Log4perl::init('etc/log4perl_tests.conf');

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

SKIP: {
  skip "$db_credentials credentials file not present", 6
    unless -e $db_credentials;

  my $db = WTSI::NPG::Genotyping::Database::Sequenom->new
    (name   => 'mspec2',
     inifile => $db_credentials);

  is($db->name, 'mspec2');
  ok($db->data_source);
  ok($db->username);
  ok($db->connect(RaiseError => 1));
  ok($db->dbh);
  ok($db->disconnect);
}
