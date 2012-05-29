use utf8;

use strict;
use warnings;

use Test::More tests => 3;
use Test::Exception;

use WTSI::Genotyping qw(update_snp_locations);

my $data_path = "t/update_snp_locations";

my $bim_in = "$data_path/input.bim";
my $bim_tmp_out = "$data_path/output." . $$;

my $locations = {'rs0001' => [1, 11],
                 'rs0002' => [1, 101],
                 'rs0004' => [2, 201]};

my $in;
my $out;
open($in, "<$bim_in") or die "Failed to open '$bim_in'\n";
open($out, ">$bim_tmp_out")
  or die "Failed to open '$bim_tmp_out' for writing\n";

dies_ok { update_snp_locations($in, $out, $locations) }
  'Expected update to fail on unknown SNP';
close($in);
close($out);

open($in, "<$bim_in") or die "Failed to open '$bim_in'\n";
open($out, ">$bim_tmp_out")
  or die "Failed to open '$bim_tmp_out' for writing\n";
$locations->{'rs0003'} = [2, 21],
my $num_updated = update_snp_locations($in, $out, $locations);
ok($num_updated == 4);

close($in);
close($out);

my @expected;
while (my $line = <DATA>) {
  chomp($line);
  push @expected, $line;
}

open($out, "<$bim_tmp_out")
  or die "Failed to open '$bim_tmp_out' for reading\n";

my @observed;
while (my $line = <$out>) {
  chomp($line);
  push @observed, $line;
}
close($out);
unlink($bim_tmp_out);

is_deeply(\@observed, \@expected);


__DATA__
1	rs0001	0	11	A	G
1	rs0002	0	101	A	G
2	rs0003	0	21	A	G
2	rs0004	0	201	A	G
