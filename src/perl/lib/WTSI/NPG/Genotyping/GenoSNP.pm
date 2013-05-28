use utf8;

package WTSI::NPG::Genotyping::GenoSNP;

use strict;
use warnings;
use Carp;

use WTSI::NPG::Utilities::DelimitedFiles;

use base 'Exporter';

our @EXPORT_OK = qw(write_gs_snps);

=head2 write_gs_snps

  Arg [1]    : filehandle
  Arg [2]    : arrayref of SNP hashes (see read_snp_json)
  Example    : $fh = write_gs_snps($fh, \@snps)
  Description: Writes SNP annotation to a filehandle in the format
               expected by GenoSNP.

               Important: The integer in the second column is not
               the Beadpool number as specified in the GenoSNP docs.
               The value written is ($snp->{norm_id} % 100) + 1. This
               is to emulate the existing software (g2i). I have no idea
               why this is not the Beadpool number.
  Returntype : filehandle
  Caller     : general
=cut

sub write_gs_snps {
  my ($fh, $snps) = @_;

  foreach my $snp (@$snps) {
    print $fh join("\t",
                   $snp->{name},
                   ($snp->{norm_id} % 100) + 1,
                   $snp->{allele_a},
                   $snp->{allele_b}), "\n";
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
