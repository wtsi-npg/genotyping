
# Tests WTSI::NPG::Genotyping

use utf8;

use strict;
use warnings;
use Cwd qw(abs_path);

use Test::More tests => 4;

BEGIN { use_ok('WTSI::NPG::Genotyping'); }
require_ok('WTSI::NPG::Genotyping');

use WTSI::NPG::Genotyping qw(base_dir
                             config_dir);

is(base_dir(), abs_path('./blib'), 'Incorrect base_dir');

is(config_dir(), abs_path('./blib/etc'), 'Incorrent config_dir');
