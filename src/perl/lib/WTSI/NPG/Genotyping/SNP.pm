
use utf8;

package WTSI::NPG::Genotyping::SNP;

use Moose;

has 'name'       => (is => 'ro', isa => 'Str', required => 1);
has 'ref_allele' => (is => 'ro', isa => 'Str', required => 0);
has 'alt_allele' => (is => 'ro', isa => 'Str', required => 0);
has 'chromosome' => (is => 'ro', isa => 'Str', required => 0);
has 'position'   => (is => 'ro', isa => 'Int', required => 0);
has 'strand'     => (is => 'ro', isa => 'Str', required => 0);
has 'str'        => (is => 'ro', isa => 'Str', required => 0);

=head2 equals

  Arg [1]    : WTSI::NPG::Genotyping::SNP

  Description: Test whether this SNP is equal to another SNP. Two SNPs are
               equal if all their attributes other than 'str' are equal.
               (Equality of 'str' is not tested, as it represents raw
               input from file.)

  Returntype : Bool

=cut

sub equals {
    my ($self, $other) = @_;
    my $equal = ($self->name eq $other->name &&
                 $self->ref_allele eq $other->ref_allele &&
                 $self->alt_allele eq $other->alt_allele &&
                 $self->chromosome eq $other->chromosome &&
                 $self->position == $other->position &&
                 $self->strand eq $other->strand
             );
    # do not compare the 'str' attribute, which represents raw input from file
    return $equal;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
