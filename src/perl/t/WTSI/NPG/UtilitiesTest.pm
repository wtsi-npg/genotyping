
use utf8;

package WTSI::NPG::UtilitiesTest;

use strict;
use warnings;
use File::Temp qw(tempfile);

use base qw(WTSI::NPG::Test);
use Test::More tests => 801;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Utilities'); }

use WTSI::NPG::Utilities qw(common_stem
                            depad_well);

sub test_common_stem : Test(11) {
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
}

sub test_depad_well : Test(789) {
  # Literally:

  is(depad_well('A01'), 'A1');
  is(depad_well('A1'),  'A1');

  dies_ok { depad_well('A00') };
  dies_ok { depad_well('A001') };

  dies_ok { depad_map(undef) };
  dies_ok { depad_map('') };
  dies_ok { depad_map(' ') };
  dies_ok { depad_map(' A01') };
  dies_ok { depad_map(' A1') };

  # Exhaustive:
  foreach my $letter (qw(A B C D E F G H I J K L M)) {
    foreach my $digit (1..20) {
      # Remove leading zero so that:
      # A01  -> A1
      # A1   -> A1
      # A10  -> A10

      my $single_padded = sprintf("%02d", $digit);
      my $double_padded = sprintf("%03d", $digit);

      is(depad_well($letter . $single_padded), $letter . $digit);
      is(depad_well($letter .         $digit), $letter . $digit);

      dies_ok { depad_well($letter . $double_padded) }
        "Unexpected well address '" . $letter . $double_padded . "'";
    }
  }
}
