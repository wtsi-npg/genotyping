
use utf8;

package WTSI::NPG::DatabaseTest;

use strict;
use warnings;
use English;
use File::Spec;

use base qw(WTSI::NPG::Test);
use Test::More tests => 3;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Database'); }

use WTSI::NPG::Database;

