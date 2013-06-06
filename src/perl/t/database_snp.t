
# Tests WTSI::NPG::Genotyping::Database::SNP

use utf8;

use strict;
use warnings;

use Test::More tests => 8;

BEGIN { use_ok('WTSI::NPG::Genotyping::Database::SNP'); }
require_ok('WTSI::NPG::Genotyping::Database::SNP');

use WTSI::NPG::Genotyping::Database::SNP;

Log::Log4perl::init('etc/log4perl_tests.conf');

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

SKIP: {
  skip "$db_credentials credentials file not present", 6
    unless -e $db_credentials;

  my $db = WTSI::NPG::Genotyping::Database::SNP->new
    (name   => 'snp',
     inifile => $db_credentials);

  is($db->name, 'snp');
  ok($db->data_source);
  ok($db->username);
  ok($db->connect(RaiseError => 1));
  ok($db->dbh);
  ok($db->disconnect);
}
