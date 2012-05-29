use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;


=head2 update_snp_locations

  Arg [1]    : filehandle
  Arg [2]    : filehandle
  Arg [3]    : hashref of locations, each key beiong a SNP name and each value
               being an arrayref of two values; chromosome and physical
               position.
  Example    : $n = update_snp_locations(\*STDIN, \*STDOUT, \%locations)
  Description: Updates a stream of Plink BIM format records with new SNP
               location (chromosome name and physical position) information
               and writes it to another stream. The chromosome names must be
               in Plink encoded numeric format.
  Returntype : integer, number of records processed
  Caller     : general

=cut

sub update_snp_locations {
  my ($in, $out, $locations) = @_;
  my $n = 0;

  while (my $line = <$in>) {
    chomp($line);
    my ($chr, $snp_name, $genetic_pos, $physical_pos, $allele1, $allele2) =
      split /\s+/, $line;

    unless (exists $locations->{$snp_name}) {
      croak "Failed to update the location of SNP '$snp_name'; " .
        "no location was provided";
    }

    my $new_loc = $locations->{$snp_name};
    unless (ref($new_loc) eq 'ARRAY' && scalar @$new_loc == 2) {
      croak "Failed to update the location of SNP '$snp_name'; " .
        "location was not a 2-element array";
    }

    my $new_chr = $new_loc->[0];
    my $new_pos = $new_loc->[1];

    print $out join("\t", $new_chr, $snp_name, $genetic_pos, $new_pos,
                    $allele1, $allele2), "\n";
    ++$n;
  }

  return $n;
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
