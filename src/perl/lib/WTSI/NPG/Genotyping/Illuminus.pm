use utf8;

package WTSI::NPG::Genotyping::Illuminus;

use strict;
use warnings;
use Carp;

use WTSI::NPG::Utilities::DelimitedFiles qw(read_column_names
                                            filter_columns);

use base 'Exporter';
our @EXPORT_OK = qw(read_it_column_names
                    update_it_columns
                    read_gt_column_names
                    filter_gt_columns
                    write_gt_calls);

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
  my $fh = shift;

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

=head2 update_it_columns

  Arg [1]    : filehandle
  Arg [2]    : filehandle
  Arg [3]    : arrayref of column indices
  Arg [4]    : value to insert
  Example    : $n = update_it_columns(\*STDIN, \*STDOUT, [0, 1], 'NaN')
  Description: Reads Illuminus intensity format data from $in and writes
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
    my @fields = split(/\t/, $line);

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


=head2 read_gt_column_names

  Arg [1]    : filehandle
  Example    : $names = read_gt_column_names($fh);
  Description: Reads the column names from a filehandle of genotype call format
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
  Arg [3]    : column separator
  Arg [4]    : column offset (0-based)
  Arg [5]    : column group (number of data per column)
  Arg [6]    : arrayref of column indices
  Arg [7]    : operation to perform on the selected columns, either 'include'
               or 'exclude'
  Example    : @filtered =
                 filter_columns(["a", "b", "c", "d"], [0, 3], 'include')
  Description: Retains or removes the indicated columns from an array of columns
               of genotype call or probability format data.
  Returntype : array
  Caller     : general

=cut

sub filter_gt_columns {
  my ($in, $out, $col_separator, $col_offset,
      $col_group, $indices, $op) = @_;

  my $num_lines = 0;
  while (my $line = <$in>) {
    chomp($line);
    my @fields = split(/$col_separator/, $line);

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
  Description: Writes genotype call format data to a filehandle, given streams
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
      split(/\s+/, $calls_str);
    my ($prob_name, $prob_pos, $prob_alleles, @probs) =
      split(/\s+/, $probs_str);

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

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
