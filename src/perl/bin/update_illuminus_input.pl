#!/software/bin/perl

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Pod::Usage;

use WTSI::Genotyping qw(maybe_stdin maybe_stdout common_stem
                        read_fon find_column_indices filter_columns
                        read_it_column_names update_it_columns);

run() unless caller();

sub run {
  my $columns;
  my $input;
  my $output;
  my $operation;
  my $value;
  my $verbose;

  GetOptions('columns=s' => \$columns,
             'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'input=s' => \$input,
             'operation=s' => \$operation,
             'output=s' => \$output,
             'value=s' => \$value,
             'verbose' => \$verbose);

  unless ($columns) {
    pod2usage(-msg => "A --columns argument is required\n",
              -exitval => 2);
  }

  unless (defined $value) {
    pod2usage(-msg => "A --value argument is required\n",
              -exitval => 2);
  }

  $operation ||= 'include';
  $operation = lc($operation);

  unless ($operation eq 'include' || $operation eq 'exclude') {
    pod2usage(-msg => "Invalid operation '$operation', expected one of " .
              "[include exclude]\n",
              -exitval => 2);
  }


  my $col;
  open($col, "<$columns")
    or die "Failed to open column file '$columns' for reading: $!\n";
  my $column_names = read_fon($col);
  close($col);

  my @samples_to_replace = @$column_names;

  my $in = maybe_stdin($input);
  my $out = maybe_stdout($output);

  # Read the intensity data column names
  my @col_names = @{read_it_column_names($in)};
  unless (@col_names % 2 == 0) {
    die "Intensity data contained an odd number of data columns\n";
  }

  # Calculate the real sample names from the intensity data column
  # names
  my @sample_names;
  for (my $i = 0; $i < scalar @col_names; $i += 2) {
    push(@sample_names, common_stem($col_names[$i], $col_names[$i + 1]));
  }

  # Validate the input
  my %sample_lookup = map { $_ => 1 } @sample_names;
  for (my $i = 0; $i < scalar @samples_to_replace; ++$i) {
    my $x = $samples_to_replace[$i];
    unless (exists $sample_lookup{$x}) {
      die "Intensity data did not contain data columns for '$x'\n";
    }
  }

  # Calculate the intensity data column indices on which to operate
  my %col_lookup = map { $_ => 1 } @samples_to_replace;
  my @indices;
  for (my $i = 0, my $j = 0; $i < scalar @sample_names; ++$i, $j += 2) {
    my $y = $sample_names[$i];

    if (($operation eq 'include' && exists $col_lookup{$y}) ||
        ($operation eq 'exclude' && ! exists $col_lookup{$y})) {
      push(@indices, $j, $j + 1);
    }
  }

  my $num_snps = update_it_columns($in, $out, \@indices, $value);

  if ($verbose) {
    my $verb = $operation . "d";
    my $num_cols = scalar @indices;
    print STDERR "$verb $num_cols columns from $num_snps records\n";
  }

  close($in);
  close($out);
}


__END__

=head1 NAME

update_illuminus_input.pl -- Modifies Illuminus (Sanger version) input
files by sample column.

=head1 SYNOPSIS

update_illuminus_input --columns <column name file> \
   [--operation include|exclude] \
   --input <intensity input file> --output <intensity output file> \
   --value <intensity value>
   [--verbose]

Options:

  --columns   A text file of column names, one per line, corresponding to
              the sample name prefix of column names in the intensity file.
              The order of the lines is not meaningful.
  --help      Display help.
  --input     The Illuminus intensity file to be read.
  --operation The operation to carry out on the selected columns. The value
              maye be either 'include' or 'exclude' (case-insensitive).
              Optional, defaults to 'include'.
  --output    The Illuminus intensity file to be written.
  --value     The new value to replace the existing values where indicated.
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Filters Illuminus (Sanger version) input by column, writing the
results to a new file. Columns to include or exclude are specified via
a text file of column names, one per line, corresponding to sample
names. All selected columns will have any existing intensity value
replaced by the value given on the command line, in both channels.
Note that the column headers in an intensity file are not sample
names; they have the sample name as a prefix.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=head1 VERSION

  0.1.0

=head1 CHANGELOG

Thu Feb  9 10:58:12 GMT 2012 -- Initial version 0.1.0

=cut
