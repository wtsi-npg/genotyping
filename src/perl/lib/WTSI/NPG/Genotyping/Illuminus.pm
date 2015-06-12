use utf8;

package WTSI::NPG::Genotyping::Illuminus;

use strict;
use warnings;
use Carp;
use List::AllUtils qw(pairs);

use WTSI::DNAP::Utilities::IO qw(maybe_stdin);
use WTSI::NPG::Utilities qw(common_stem);
use WTSI::NPG::Utilities::DelimitedFiles qw(read_column_names
                                            find_column_indices
                                            filter_columns);

use base 'Exporter';
our @EXPORT_OK = qw(filter_gt_columns
                    find_female_columns
                    get_it_sample_names
                    nullify_females
                    read_gt_column_names
                    read_it_column_names
                    update_it_columns
                    write_it_header
                    write_gt_calls);

our $VERSION = '';

=head2 read_it_column_names

  Arg [1]    : filehandle
  Example    : @names = read_it_column_names(\*STDIN, "\t")
  Description: This function is designed to read sample column name headers
               from an Illuminus intensity format file. The annotation columns
               are ignored.
  Returntype : arrayref
  Caller     : general

=cut

sub read_it_column_names {
  my ($fh) = @_;

  my @names = read_column_names($fh, "\t");

  my @annotation_columns = qw(SNP Coor Alleles);
  for (my $i = 0; $i < scalar @annotation_columns; ++$i) {
    my $annot = $annotation_columns[$i];
    unless ($names[$i] eq $annot) {
       confess "Malformed column header line (missing the '$annot' column)\n";
    }
  }

  @names = @names[3..$#names];

  return \@names;
}

sub get_it_sample_names {
  my ($column_names) = @_;

  my $num_columns = scalar @$column_names;
  unless ($num_columns % 2 == 0) {
    die "Intensity data contained an odd number of columns: $num_columns\n";
  }

  my @sample_names;
  foreach (pairs @$column_names) {
    my ($name_a, $name_b) = @$_;
    push @sample_names, common_stem($name_a, $name_b);
  }

  return \@sample_names;
}

sub write_it_header {
  my ($fh, $column_names) = @_;

  my @annotation_columns = qw(SNP Coor Alleles);
  print $fh join("\t", @annotation_columns, @$column_names), "\n";

  return scalar @$column_names;
}

=head2 update_it_columns

  Arg [1]    : filehandle
  Arg [2]    : filehandle
  Arg [3]    : arrayref of column indices
  Arg [4]    : value to insert
  Example    : $n = update_it_columns(\*STDIN, \*STDOUT, [0, 1], 'NaN')
  Description: Read Illuminus intensity format data from $in and writes
               it to $out, having changed the intensity values in columns
               denoted by the indices to contain the specified value. The
               column indices count from the first sample column. i.e. the
               annotation columns are ignored (but are written unchanged).
  Returntype : arrayref
  Caller     : general

=cut

sub update_it_columns {
  my ($in, $out, $indices, $value) = @_;

  my @annotation_columns = qw(SNP Coor Alleles);
  my %index_lookup = map { $_ => 1} @$indices;

  my $num_lines = 0;
  while (my $line = <$in>) {
    chomp($line);

    my @fields = split(/\t/msx, $line);

    for (my $i = 0; $i < scalar @annotation_columns; ++$i) {
      my $annot = shift @fields;
      print $out "$annot\t";
    }

    for (my $i = 0; $i < scalar @fields; ++$i) {
      if (exists $index_lookup{$i}) {
        $fields[$i] = $value;
      }
    }

    print $out join("\t", @fields), "\n";
    ++$num_lines;
  }

  return $num_lines;
}

sub find_female_columns {
  my ($column_names, $samples) = @_;

  my @females = grep { $_->{'gender'} eq 'Female'} @$samples;
  my @female_sample_names = map { $_->{'uri'} } @females;

  # Calculate the real sample names from the intensity data column
  # names
  my $sample_names = get_it_sample_names($column_names);

  # Validate the input
  my %sample_lookup = map { $_ => 1 } @$sample_names;
  foreach my $fname (@female_sample_names) {
    unless (exists $sample_lookup{$fname}) {
      die "Intensity data did not contain columns for '$fname'\n";
    }
  }

  # Calculate the intensity data column indices on which to operate
  my %female_lookup = map { $_ => 1 } @female_sample_names;
  my @female_column_indices;

  my $column_a_index = 0;
  foreach my $sname (@$sample_names) {
    my $column_b_index = $column_a_index + 1;

    if (exists $female_lookup{$sname}) {
      push(@female_column_indices, $column_a_index, $column_b_index);
    }

    $column_a_index += 2;
  }

  return \@female_column_indices;
}

sub nullify_females {
  my ($input, $command, $samples, $verbose) = @_;

  my $fh = maybe_stdin($input);

  my $column_names = read_it_column_names($fh);
  my $females = find_female_columns($column_names, $samples);

  if ($verbose) {
    print STDERR "Nullifying intensities for females in columns: [",
      join(", ", @$females), "]", "\n";
  }

  open(my $out, '|-', "$command") or die "Failed to open pipe to '$command'\n";
  write_it_header($out, $column_names);

  my $num_rows = update_it_columns($fh, $out, $females, '1.000');
  close($out) or warn "Failed to close pipe to '$command'\n";

  return $num_rows;
}

=head2 read_gt_column_names

  Arg [1]    : filehandle
  Example    : $names = read_gt_column_names($fh);
  Description: Read the column names from a filehandle of genotype call format
               data.
  Returntype : arrayref
  Caller     : general

=cut

sub read_gt_column_names {
  my ($fh) = @_;

  my @names = read_column_names($fh, "\t");

  unless ($names[0] eq '') {
    confess "Malformed column header line (missing the empty left column)\n";
  }

  @names = @names[1..$#names];

  return \@names;
}

=head2 filter_gt_columns

  Arg [1]    : input filehandle
  Arg [2]    : output filehandle
  Arg [3]    : column separator (permitted are \s or \t)
  Arg [4]    : column offset (0-based)
  Arg [5]    : column group (number of data per column)
  Arg [6]    : arrayref of column indices
  Arg [7]    : operation to perform on the selected columns, either 'include'
               or 'exclude'
  Example    : @filtered =
                 filter_columns(["a", "b", "c", "d"], [0, 3], 'include')
  Description: Retain or removes the indicated columns from an array of columns
               of genotype call or probability format data.
  Returntype : array
  Caller     : general

=cut

sub filter_gt_columns {
  my ($in, $out, $col_separator, $col_offset,
      $col_group, $indices, $op) = @_;

  defined $col_separator or
    confess 'A defined col_separator argument is required';

  my $split_regex;
  if ($col_separator eq ' ') {
    $split_regex = qr{\s}msx;
  }
  elsif ($col_separator eq "\t") {
    $split_regex = qr{\t}msx;
  }
  else {
    confess "Invalid col_separator argument '$col_separator'. " .
      "A single tab or space are permitted.";
  }

  my $num_lines = 0;
  while (my $line = <$in>) {
    chomp($line);
    my @fields = split(/$split_regex/msx, $line);

    for (my $i = 0; $i < $col_offset; ++$i) {
      my $annotation_col = shift @fields;
      print $out "$annotation_col$col_separator";
    }

    # Handle probability data which come in adjacent groups of columns
    # that correspond to a single column of genotype data.
    my @groups;
    if ($col_group == 1) {
      @groups = @fields;
    } else {
      for (my $i = 0; $i < scalar @fields; $i += $col_group) {

        my @group;
        for (my $j = 0; $j < $col_group; ++$j) {
          push(@group, $fields[$i + $j]);
        }

        push(@groups, join($col_separator, @group));
      }
    }

    my @selected = filter_columns(\@groups, $indices, $op);

    print $out join($col_separator, @selected), "\n";

    ++$num_lines;
  }

  return $num_lines;
}

=head2 write_gt_calls

  Arg [1]    : input filehandle (raw calls))
  Arg [2]    : input filehandle (raw probabilities)
  Arg [3]    : output filehandle
  Example    : $num_records = write_gt_calls($cin, $pin, \*STDOUT)
  Description: Write genotype call format data to a filehandle, given streams
               of raw Illuminus results (calls codes and probabilities)
  Returntype : integer
  Caller     : general

=cut

sub write_gt_calls {
  my ($calls_in, $probs_in, $out) = @_;

  my $num_records = 0;
  my $calls_str = <$calls_in>;
  my $probs_str = <$probs_in>;

  if (defined $calls_str && defined $probs_str) {
    chomp($calls_str);
    chomp($probs_str);

    my ($call_name, $call_pos, $call_alleles, @calls) =
      split(/\s+/msx, $calls_str);
    my ($prob_name, $prob_pos, $prob_alleles, @probs) =
      split(/\s+/msx, $probs_str);

    unless ($call_name eq $prob_name &&
            $call_pos == $prob_pos &&
            $call_alleles eq $prob_alleles) {
      confess "Illuminus calls and probabilities are out of sync: " .
        "$call_name/$prob_name " .
          "$call_pos/$prob_pos " .
            "$call_alleles/$prob_alleles\n"
    }

    my $num_calls = scalar @calls;
    my $num_probs = scalar @probs;

    unless ($num_calls * 4 == $num_probs) {
      confess "Illuminus calls and probabilities are out of sync: " .
        "$num_calls calls, $num_probs probabilities, " .
          "# probabilities was not equal to  4 * # calls\n";
    }

    my ($allele_a, $allele_b) = (substr($call_alleles, 0, 1),
                                 substr($call_alleles, 1, 1));
    my @genotypes = ($allele_a . $allele_a,
                     $allele_a . $allele_b,
                     $allele_b . $allele_b,
                     "NN");

    print $out "$call_name\t";
    for (my $i = 0, my $j = 0; $i < $num_calls; ++$i, $j += 4) {
      # The call is an integer code that indicates which genotype has
      # been chosen and which of the 4 probabilities to include in the
      # output
      my $k = $calls[$i] - 1;
      my $g = $genotypes[$k];
      my $p = $probs[$j + $k];

      # If we have no call, we are certain that we have none
      if ($g eq 'NN') {
        $p = 1;
      }

      print $out "\t$g;$p";
    }
    print $out "\n";

    ++$num_records;
  }
  elsif ($calls_str && ! defined $probs_str) {
    chomp($calls_str);
    confess "Illuminus call data is missing probabilites: '$calls_str'\n";
  }
  elsif ($probs_str && ! defined $calls_str) {
    chomp($probs_str);
    confess "Illuminus probabilities found withhout call data: '$probs_str'\n";
  }

  return $num_records;
}

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
