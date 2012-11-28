# Tests WTSI::Genotyping::Utilities

use utf8;

use strict;
use warnings;
use Test::More tests => 11;

use WTSI::Genotyping qw(common_stem);

# common_stem
is(common_stem('', ''), '');
is(common_stem('a', ''), '');
is(common_stem('', 'a'), '');
is(common_stem('a', 'a'), 'a');

is(common_stem('aa', 'a'), 'a');
is(common_stem('a', 'aa'), 'a');
is(common_stem('ab', 'a'), 'a');
is(common_stem('a', 'ab'), 'a');

is(common_stem('aa', 'aa'), 'aa');
is(common_stem('aa', 'bb'), '');

is(common_stem('abc123', 'abc456'), 'abc');
