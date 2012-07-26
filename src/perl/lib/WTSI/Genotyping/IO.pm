use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;
use JSON;


=head2 maybe_stdin

  Arg [1]    : filename or undef
  Example    : $fh = maybe_stdin($name)
  Description: Returns a filehandle for reading. If the argument is a
               filename, opens that file and returns the filehandle. If
               the argument is undef, returns STDIN.
  Returntype : filehandle
  Caller     : general

=cut

sub maybe_stdin {
  my $file = shift;

  my $fh;
  if (defined $file) {
    unless (-e $file) {
      croak "file '$file' does not exist\n";
    }

    unless (-r $file) {
      croak "file '$file' is not readable\n";
    }

    if (-d $file) {
      croak "'$file' is a directory\n";
    }

    open($fh, '<', "$file") or confess "Failed to open file '$file': $!\n";
  } else {
    $fh = \*STDIN;
  }

  return $fh;
}

=head2 maybe_stdout

  Arg [1]    : filename or undef
  Example    : $fh = maybe_stdout($name)
  Description: Returns a filehandle for writing. If the argument is a
               filename, opens that file and returns the filehandle. If
               the argument is undef, returns STDOUT.
  Returntype : filehandle
  Caller     : general

=cut

sub maybe_stdout {
  my $file = shift;

  my $fh;
  if (defined $file) {
    open($fh, '>', "$file") or confess "Failed to open '$file': $!\n";
  } else {
    $fh = \*STDOUT;
  }

  return $fh;
}

=head2 read_sample_json

  Arg [1]    : filename
  Example    : @samples = read_sample_json($file)
  Description: Returns sample metadata hashes, one per sample, from a JSON file.
  Returntype : array
  Caller     : general

=cut

sub read_sample_json {
  my $file = shift;

  open(my $fh, '<', "$file")
    or confess "Failed to open JSON file '$file' for reading: $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close($fh) or warn "Failed to close JSON file '$file'\n";

  return @{from_json($str, {utf8 => 1})};
}

1;

=head2 read_snp_json

  Arg [1]    : filename
  Example    : @snps = read_snp_json($file)
  Description: Returns SNP metadata hashes, one per SNP, from a JSON file.
  Returntype : array
  Caller     : general

=cut

sub read_snp_json {
  my $file = shift;

  open(my $fh, '<', "$file")
    or confess "Failed to open JSON file '$file' for reading: $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close($fh) or warn "Failed to close JSON file '$file'\n";

  return @{from_json($str, {utf8 => 1})};
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
