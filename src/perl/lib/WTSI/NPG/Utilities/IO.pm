use utf8;

package WTSI::NPG::Utilities::IO;

use strict;
use warnings;
use Carp;

use base 'Exporter';
our @EXPORT_OK = qw(maybe_stdin maybe_stdout);

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
  my ($file) = @_;

  my $fh;
  if (defined $file) {
    unless (-e $file) {
      croak "file '$file' does not exist\n";
    }

    unless (-r $file) {
      # Gives incorrect result on mounted Windows shares
      # croak "file '$file' is not readable\n";
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
  my ($file) = @_;

  my $fh;
  if (defined $file) {
    open($fh, '>', "$file") or confess "Failed to open '$file': $!\n";
  } else {
    $fh = \*STDOUT;
  }

  return $fh;
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
