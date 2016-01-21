
use utf8;

package WTSI::NPG::Genotyping::IlluminusTest;

use strict;
use warnings;
use File::Compare;
use File::Temp qw(tempdir);
use JSON;

use base qw(WTSI::NPG::Test);
use Test::More tests => 11;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Illuminus'); }

use WTSI::NPG::Genotyping::Illuminus qw(find_female_columns
                                        get_it_sample_names
                                        nullify_females
                                        read_it_column_names
                                        update_it_columns
                                        write_it_header);

use WTSI::NPG::Utilities qw(common_stem);

my $data_path = './t/illuminus';

sub test_read_it_column_names : Test(1) {
  my @column_names = ('urn:wtsi:example_0000A',
                      'urn:wtsi:example_0000B',
                      'urn:wtsi:example_0001A',
                      'urn:wtsi:example_0001B',
                      'urn:wtsi:example_0002A',
                      'urn:wtsi:example_0002B',
                      'urn:wtsi:example_0003A',
                      'urn:wtsi:example_0003B',
                      'urn:wtsi:example_0004A',
                      'urn:wtsi:example_0004B');

  my $iln_file = "$data_path/example_all.iln";

  open my $fh, '<', $iln_file or die "Failed to open '$iln_file': $!\n";
  my $columns = read_it_column_names($fh);
  close $fh;

  is_deeply($columns, \@column_names) or diag explain $columns;
}

sub test_get_it_sample_names : Test(2) {
  my @column_names = ('urn:wtsi:example_0000A',
                      'urn:wtsi:example_0000B',
                      'urn:wtsi:example_0001A',
                      'urn:wtsi:example_0001B',
                      'urn:wtsi:example_0002A',
                      'urn:wtsi:example_0002B',
                      'urn:wtsi:example_0003A',
                      'urn:wtsi:example_0003B',
                      'urn:wtsi:example_0004A',
                      'urn:wtsi:example_0004B');

  my @odd_columns = @column_names[0..8];
  dies_ok { get_it_sample_names(\@odd_columns) } 'Odd number of names';

  my $sample_names = get_it_sample_names(\@column_names);

  is_deeply($sample_names, ['urn:wtsi:example_0000',
                            'urn:wtsi:example_0001',
                            'urn:wtsi:example_0002',
                            'urn:wtsi:example_0003',
                            'urn:wtsi:example_0004'])
    or diag explain $sample_names;
}

sub test_find_female_columns :Test(1) {
  my @column_names = ('urn:wtsi:example_0000A',
                      'urn:wtsi:example_0000B',
                      'urn:wtsi:example_0001A',
                      'urn:wtsi:example_0001B',
                      'urn:wtsi:example_0002A',
                      'urn:wtsi:example_0002B',
                      'urn:wtsi:example_0003A',
                      'urn:wtsi:example_0003B',
                      'urn:wtsi:example_0004A',
                      'urn:wtsi:example_0004B');

  my $sample_json = "$data_path/example.json";

  open my $fh, '<', $sample_json or die "Failed to open '$sample_json': $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close $fh;

  my $samples = JSON->new->utf8->decode($str);
  my $female_indices = find_female_columns(\@column_names, $samples);

  # Samples 0, 2 and 4 of 5 are female. There are two columns per
  # sample
  my @expected = (0, 1, 4, 5, 8, 9);

  is_deeply($female_indices, \@expected) or diag explain \$female_indices;
}

sub test_nullify_females : Test(2) {
  my $expected_file = "$data_path/example_null_females.iln";

  my $sample_json = "$data_path/example.json";

  open my $fh, '<', $sample_json or die "Failed to open '$sample_json': $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close $fh;

  my $samples = JSON->new->utf8->decode($str);

  my $tmpdir = tempdir(CLEANUP => 1);
  my $out_file = "$tmpdir/example_null_females.txt";

  my $command = "cat - > $out_file";
  my $illuminus_input = "$data_path/example_all.iln";

  my $num_rows = nullify_females($illuminus_input, $command, $samples);

  cmp_ok($num_rows, '==', 10, 'Number of rows updated');
  ok(compare($out_file, $expected_file) == 0,
     "$out_file is identical to $expected_file");
}

sub test_write_it_header : Test(2) {
  my $expected_file = "$data_path/iln_header.txt";

  my $tmpdir = tempdir(CLEANUP => 1);
  my $out_file = "$tmpdir/iln_header.txt";

  open my $out, '>', $out_file or die "Failed to open '$out_file': $!\n";
  my $num_data_columns = write_it_header($out, ['a', 'b', 'c']);
  close $out;

  cmp_ok($num_data_columns, '==', 3, 'Number of data columns');
  ok(compare($out_file, $expected_file) == 0,
     "$out_file is identical to $expected_file");
}

sub test_update_it_columns : Test(2) {
  my $illuminus_input = "$data_path/example_all.iln";
  my $expected_file = "$data_path/example_all_updated_columns.iln";

  open my $in, '<', $illuminus_input
    or die "Failed to open '$illuminus_input': $!\n";

  # Read and discard the header in this test
  my $columns = read_it_column_names($in);

  my $tmpdir = tempdir(CLEANUP => 1);
  my $out_file = "$tmpdir/example_all.iln";

  open my $out, '>', $out_file or die "Failed to open '$out_file': $!\n";

  my $num_data_columns = write_it_header($out, $columns);
  my $num_rows = update_it_columns($in, $out, [0, 1, 4, 5, 8, 9], 'NaN');

  close $in;
  close $out;

  cmp_ok($num_rows, '==', 10, 'Number of rows updated');
  ok(compare($out_file, $expected_file) == 0,
     "$out_file is identical to $expected_file");
}

1;
