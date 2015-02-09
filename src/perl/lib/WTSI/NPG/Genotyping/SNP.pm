
use utf8;

package WTSI::NPG::Genotyping::SNP;

use Moose;

use WTSI::NPG::Genotyping::Types qw(:all);

has 'name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'ref_allele' =>
  (is       => 'ro',
   isa      => DNABase,
   required => 0);

has 'alt_allele' =>
  (is       => 'ro',
   isa      => DNABase,
   required => 0);

has 'chromosome' =>
  (is       => 'ro',
   isa      => HsapiensChromosome,
   required => 0);

has 'position' =>
  (is       => 'ro',
   isa      => 'Int',
   required => 0);

has 'strand' =>
  (is       => 'ro',
   isa      => DNAStrand,
   required => 0);

has 'str'=>
  (is       => 'ro',
   isa      => 'Str',
   required => 0);

has 'snpset' =>
  (is       => 'rw',
   isa      => SNPSet,
   required => 0,
   weak_ref => 1);

=head2 is_gender_marker

  Arg [1]    : None

  Description: Return true if this SNP is actually a gender marker.

  Returntype : Bool

=cut

sub is_gender_marker {
  my ($self) = @_;

  return $self->name =~ m{^GS};
}

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

=head1 NAME

WTSI::NPG::Genotyping::SNP - Information on a single SNP

=head1 SYNOPSIS

   my $snp = WTSI::NPG::Genotyping::SNP(name => 'rs12345');

=head1 DESCRIPTION

A instance of SNP represents a SNP on a specific reference (or
references).

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
