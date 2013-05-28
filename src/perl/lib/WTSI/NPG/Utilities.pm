use utf8;

package WTSI::NPG::Utilities;

use strict;
use warnings;

use base 'Exporter';
our @EXPORT_OK = qw(common_stem trim);

=head2 common_stem

  Arg [1]    : string
  Arg [2]    : string
  Example    : $stem = common_stem("foo13240a", "foo199")
  Description: Returns the common part of the two arguments, starting
               from the left (index 0). If one or more of the arguments
               are empty strings, or the arguments differ at the first
               character, an empty string is returned.
  Returntype : string
  Caller     : general

=cut

sub common_stem {
  my ($str1, $str2) = @_;
  my $stem = '';

  my $len1 = length($str1);
  my $len2 = length($str2);
  my $end;
  if ($len1 < $len2) {
    $end = $len1;
  } else {
    $end = $len2;
  }

  for (my $i = 0; $i < $end; ++$i) {
    my $c1 = substr($str1, $i, 1);
    my $c2 = substr($str2, $i, 1);

    last if $c1 ne $c2;

    $stem .= $c1;
  }

  return $stem;
}

=head2 trim

  Arg [1]    : string
  Example    : $trimmed = trim("  foo ");
  Description: Trim leading and trailing whitespace, withina  line, from a copy
               of the argument. Return the trimmed string.
  Returntype : string
  Caller     : general

=cut

sub trim {
  my ($str) = @_;

  my $copy = $str;
  $copy =~ s/^\s*//;
  $copy =~ s/\s*$//;

  return $copy;
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
