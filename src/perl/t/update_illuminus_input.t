
use Test::More tests => 22;

use WTSI::Genotyping qw(read_it_column_names update_it_columns);

my $data_path = "t/update_illuminus_input";

die "No data path given" unless $data_path;

my $test_intensities = "$data_path/illuminus_intensities.txt";

my $it;
open($it, "<$test_intensities") or die "Failed to open '$test_intensities'\n";

my @it_cols;
foreach my $i (0..4) {
  push(@it_cols, sprintf("sample_%02dA", $i));
  push(@it_cols, sprintf("sample_%02dB", $i));
}

is_deeply(\@it_cols, read_it_column_names($it),
          "Test reading intensity column headers");

close($it);

# Test updating
my $it_tmp = "$test_intensities." . $$;

# Test intensity update
open($ii, "<$test_intensities") or die "Failed to open '$test_intensities'\n";
open($io, ">$it_tmp") or die "Failed to open '$it_tmp' for writing\n";

my @nan_indices = (0, 1, 4, 5, 8, 9);
my @num_indices = (2, 3, 6, 7);

# update_it_columns
read_it_column_names($ii);
my $num = update_it_columns($ii, $io, \@nan_indices, 'NaN');
ok($num == 10);
close($io);

open($io, "<$it_tmp") or die "Failed to open '$it_tmp' for reading\n";

while (my $line = <$io>) {
  chomp($line);
  # Skip annotation
  my @fields = split(/\t/, $line);
  @fields = @fields[3..$#fields];

  my @nan;
  my @num;
  for (my $i = 0; $i < scalar @fields; ++$i) {
    if ($fields[$i] =~ /NaN/) {
      push(@nan, $i);
      print "$i\n";
    } else {
      push(@num, $i);
    }
  }

  is_deeply(\@nan_indices, \@nan);
  is_deeply(\@num_indices, \@num);
}

close($io);
unlink($it_tmp);
