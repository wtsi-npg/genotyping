
# Tests functions used by publish_expression_analysis.pl

use utf8;

use strict;
use warnings;

use Test::More tests => 106;

use WTSI::NPG::Expression::Publication qw(parse_beadchip_table);

Log::Log4perl::init('etc/log4perl_tests.conf');

my $data_path = "t/publish_expression_analysis";

my $beadchip_table = "$data_path/beadchip_table_example.txt";
my @samples;
open(my $in, '<', $beadchip_table)
  or die "Failed to open $beadchip_table; $!\n";
@samples = parse_beadchip_table($in);
close($in);

my $num_samples = 21;
is(scalar @samples, $num_samples);

my @expected_keys = qw(sanger_sample_id
                       infinium_plate
                       infinium_well
                       beadchip
                       beadchip_section);

foreach my $key (@expected_keys) {
  for (my $i = 0; $i < $num_samples; ++$i) {
    ok(defined $samples[$i]->{$key},
       "Missing value for key '$key' in sample $i");
  }
}
