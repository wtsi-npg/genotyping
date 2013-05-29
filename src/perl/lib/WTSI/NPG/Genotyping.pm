use utf8;

package WTSI::NPG::Genotyping;

use warnings;
use strict;
use Carp;
use JSON;

use base 'Exporter';
our @EXPORT_OK = qw(read_sample_json
                    read_snp_json);

=head2 read_sample_json

  Arg [1]    : filename
  Example    : @samples = read_sample_json($file)
  Description: Return sample metadata hashes, one per sample, from a JSON file.
  Returntype : array
  Caller     : general

=cut

sub read_sample_json {
  my ($file) = @_;

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
  Description: Return SNP metadata hashes, one per SNP, from a JSON file.
  Returntype : array
  Caller     : general

=cut

sub read_snp_json {
  my ($file) = @_;

  open(my $fh, '<', "$file")
    or confess "Failed to open JSON file '$file' for reading: $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close($fh) or warn "Failed to close JSON file '$file'\n";

  return @{from_json($str, {utf8 => 1})};
}

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping

=head1 DESCRIPTION

General purpose utilities that may be used by genotyping projects.
See individual POD for details.

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

  0.2.0

=cut
