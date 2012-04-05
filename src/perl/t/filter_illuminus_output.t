
# Tests functions used by filter_illuminus_ouput.pl

use utf8;

use strict;
use warnings;

use Test::More tests => 13;

use WTSI::Genotyping qw(read_fon find_column_indices filter_columns
                        read_gt_column_names filter_gt_columns);

my $data_path = "t/filter_illuminus_output";

die "No data path given" unless $data_path;

my $test_genotypes = "$data_path/illuminus_genotypes_1.txt";
my $test_probabilites = "$data_path/illuminus_probabilities_1.txt";
my $test_columns = "$data_path/columns.txt";

my @test_cols = qw(sample_01 sample_03 sample_05 sample_07 sample_09
                   sample_11 sample_13 sample_15 sample_17 sample_19);

# Test read_gt_column_names
my $gt;
open($gt, "<$test_genotypes") or die "Failed to open '$test_genotypes'\n";

my @gt_cols;
foreach my $i (0..19) {
  push(@gt_cols, sprintf("sample_%02d", $i));
}
is_deeply(\@gt_cols, read_gt_column_names($gt),
          "Test reading genotype column headers");

close($gt);


# Test read_fon
my $cols;
open($cols, "<$test_columns") or die "Failed to open '$test_columns'\n";
is_deeply(\@test_cols, read_fon($cols), "Test reading column names");
close($cols);


# Test finding column indices
open($gt, "<$test_genotypes") or die "Failed to open '$test_genotypes'\n";
my $gt_cols = read_gt_column_names($gt);
close($gt);

open($cols, "<$test_columns") or die "Failed to open '$test_columns'\n";
my $col_names = read_fon($cols);
close($cols);

my @indices = (1, 3, 5, 7, 9, 11, 13, 15, 17, 19);
is_deeply(\@indices, find_column_indices($col_names, $gt_cols),
          "Test finding column indices");

# Test filtering
my $gt_tmp_inc = "$test_genotypes.include." . $$;
my $gt_tmp_exc = "$test_genotypes.exclude." . $$;

my $gt_separator = "\t";
my $gt_offset = 1; # 1 leading column in genotype files
my $gt_col_group = 1; # Genotype data has one column per sample
my $op;

my $pr_tmp_inc = "$test_probabilites.include." . $$;
my $pr_tmp_exc = "$test_probabilites.exclude." . $$;

my $pr_separator = " ";
my $pr_offset = 3; # 3 leading columns in probability files
my $pr_col_group = 4; # Probability data comes in groups of 4 columns per sample


# Test genotype include filter
my $gto;
open($gt, "<$test_genotypes") or die "Failed to open '$test_genotypes'\n";
open($gto, ">$gt_tmp_inc") or die "Failed to open '$gt_tmp_inc' for writing\n";
$op = 'include';
my $headers = read_gt_column_names($gt);
my $cols_to_use = find_column_indices(\@test_cols, $headers);

print $gto $gt_separator,
  join($gt_separator, filter_columns($headers, $cols_to_use, $op)), "\n";

my $num_genotypes =
  filter_gt_columns($gt, $gto, $gt_separator, $gt_offset, $gt_col_group,
                    $cols_to_use, $op);
ok($num_genotypes == 100, "Test number of genotype rows filtered (include)");
close($gto);
close($gt);

open($gto, "<$gt_tmp_inc") or die "Failed to open '$gt_tmp_inc' for reading\n";
my $new_headers = read_gt_column_names($gto);
my $new_gt = "rs2055204	AG;1	GG;1	GG;0.9981	GG;1	AG;1	GG;1	AG;1	GG;1	AG;0.9652	GG;1\n";

is_deeply($new_headers, \@test_cols, "Test genotype columns (include)");
is($new_gt, <$gto>, "Test row of genotypes (include)");
close($gto);
unlink($gt_tmp_inc);


# Test probabilities include filter
my $pr;
my $pro;
open($pr, "<$test_probabilites") or die "Failed to open '$test_probabilites'\n";
open($pro, ">$pr_tmp_inc") or die "Failed to open '$pr_tmp_inc' for writing\n";
my $num_probs =
  filter_gt_columns($pr, $pro, $pr_separator, $pr_offset, $pr_col_group,
                    $cols_to_use, $op);
ok($num_probs == 100, "Test number of prob rows filtered (include)");
close($pro);
close($pr);

open($pro, "<$pr_tmp_inc") or die "Failed to open '$pr_tmp_inc' for reading\n";
my $new_pr = "rs2055204 2280661 AG 1.05e-07 1 0 2.598e-07 0 0 1 1.568e-06 0 0.001919 0.9981 2.979e-06 0 9.884e-09 1 1.244e-06 2.656e-07 1 0 6.193e-07 0 5.207e-10 1 6.225e-07 1.611e-07 1 0 6.568e-07 0 0 1 6.884e-07 0 0.9652 0.03482 6.041e-07 0 1.493e-12 1 7.499e-07\n";
is($new_pr, <$pro>, "Test probility columns (include)");
close($pro);
unlink($pr_tmp_inc);


# Test genotype exclude filter
open($gt, "<$test_genotypes") or die "Failed to open '$test_genotypes'\n";
open($gto, ">$gt_tmp_exc") or die "Failed to open '$gt_tmp_exc' for writing\n";
$op = 'exclude';
$headers = read_gt_column_names($gt);
$cols_to_use = find_column_indices(\@test_cols, $headers);

print $gto $gt_separator,
  join($gt_separator, filter_columns($headers, $cols_to_use, $op)), "\n";

$num_genotypes =
  filter_gt_columns($gt, $gto, $gt_separator, $gt_offset, $gt_col_group,
                    $cols_to_use, $op);
ok($num_genotypes == 100, "Test number of rows filtered (exclude)");
close($gto);
close($gt);

open($gto, "<$gt_tmp_exc") or die "Failed to open '$gt_tmp_exc' for reading\n";
$new_headers = read_gt_column_names($gto);
$new_gt = "rs2055204	GG;1	AG;0.9685	GG;1	AG;0.9777	GG;1	GG;1	GG;1	GG;1	NN;2.681e-06	NN;2.826e-06\n";

my @excluded_cols = qw(sample_00 sample_02 sample_04 sample_06 sample_08
                       sample_10 sample_12 sample_14 sample_16 sample_18);
is_deeply($new_headers, \@excluded_cols, "Test genotype columns (exclude)");
is($new_gt, <$gto>, "Test row of genotypes (exclude)");
close($gto);


# Test probility exclude filter
open($pr, "<$test_probabilites") or die "Failed to open '$test_probabilites'\n";
open($pro, ">$pr_tmp_exc") or die "Failed to open '$pr_tmp_exc' for writing\n";
$num_probs =
  filter_gt_columns($pr, $pro, $pr_separator, $pr_offset, $pr_col_group,
                    $cols_to_use, $op);
ok($num_probs == 100, "Test number of prob rows filtered (exclude)");
close($pro);
close($pr);
unlink($gt_tmp_exc);

open($pro, "<$pr_tmp_exc") or die "Failed to open '$pr_tmp_exc' for reading\n";
$new_pr = "rs2055204 2280661 AG 0 3.584e-10 1 5.587e-07 0 0.9685 0.03147 2.17e-07 0 0 1 2.747e-06 0 0.9777 0.02226 1.763e-07 0 1.194e-12 1 5.621e-07 0 0 1 1.884e-06 0 9.049e-07 1 2.127e-06 0 0 1 7.256e-07 0 0.4726 0.5274 2.681e-06 0 0.6897 0.3103 2.826e-06\n";
is($new_pr, <$pro>, "Test probility columns (exclude)");
close($pro);
unlink($pr_tmp_exc);
