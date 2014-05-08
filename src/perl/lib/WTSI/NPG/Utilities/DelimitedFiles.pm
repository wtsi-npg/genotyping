use utf8;

package WTSI::NPG::Utilities::DelimitedFiles;

use strict;
use warnings;
use Carp;

use base 'Exporter';
our @EXPORT_OK = qw(read_fon
                    read_column_names
                    find_column_indices
                    filter_columns);

=head2 read_fon

  Arg [1]    : filehandle
  Example    : $names = read_fon(*\STDIN)
  Description: Reads a file of names, one name per line. Ignores empty lines.
               Trims leading and trailing whitespace.
  Returntype : arrayref
  Caller     : general

=cut

sub read_fon {
  my ($fh) = @_;

  my @names;
  while (my $line = <$fh>) {
    chomp($line);
    $line =~ s/^\s+//;
    $line =~ s/\s+$//;
    next if $line eq '';

    push(@names, $line);
  }

  return \@names;
}

=head2 read_column_names

  Arg [1]    : filehandle
  Arg [2]    : delimiter between columns
  Arg [3]    : start column (indexed from 0), optional
  Arg [4]    : end column (indexed from 0), optional
  Example    : @names = read_column_names(\*STDIN, "\t");
               @names = read_column_names(\*STDIN, "\t", 1, 5);
  Description: This function is designed to read column name headers
               from a delimited text file.
  Returntype : array
  Caller     : general

=cut

sub read_column_names {
  my ($fh, $delimiter, $start, $end) = @_;

  my $line = <$fh>;
  unless ($line) {
    confess "Failed to find column name line\n";
  }

  chomp($line);

  my @names = split /$delimiter/, $line;
  $start ||= 0;
  $end ||= scalar @names -1;

  return @names[$start..$end];
}

=head2 find_column_indices

  Arg [1]    : arrayref of names to find
  Arg [2]    : arrayref of column names
  Example    : $indices = find_column_indices(["a", "c"], ["a", "b", "c", "d"])
  Description: Finds the indices of names within column names. Raises and error
               if any of the names are not present.
  Returntype : arrayref
  Caller     : general

=cut

sub find_column_indices {
  my ($names, $column_names) = @_;

  my $num_cols = scalar @$names;
  my $num_headers = scalar @$column_names;

  if ($num_cols > $num_headers) {
    confess "Invalid arguments: cannot find $num_cols columns in a " .
      "a total of of $num_headers columns\n";
  }

  my %header_lookup;
  my $header_index = 0;

  for (my $i = 0; $i < $num_headers; ++$i) {
    $header_lookup{$column_names->[$i]} = $i;
  }

  my @found;
  foreach my $name (@$names) {
    unless (exists $header_lookup{$name}) {
      confess "Unable to find column '$name' because it does not exist\n";
    }

    push(@found, $header_lookup{$name});
  }

  return [sort { $a <=> $b} @found];
}

=head2 filter_columns

  Arg [1]    : arrayref of column data
  Arg [2]    : arrayref of column indices
  Arg [3]    : operation to perform on the selected columns, either 'include'
               or 'exclude'
  Example    : @filtered =
                 filter_columns(["a", "b", "c", "d"], [0, 3], 'include')
  Description: Retains or removes the indicated columns from an array of columns
  Returntype : array
  Caller     : general

=cut

sub filter_columns {
  my ($columns, $indices, $op) = @_;

  my @remaining = @$columns;
  my @removed;

  foreach my $i (reverse @$indices) {
    push(@removed, splice(@remaining, $i, 1));
  }
  @removed = reverse @removed;

  my @result;
  if ($op eq 'include') {
    @result = @removed;
  } elsif ($op eq 'exclude') {
    @result = @remaining;
  } else {
    confess "Invalid operation '$op'; expected one of [include, exclude]\n";
  }

  return @result;
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
