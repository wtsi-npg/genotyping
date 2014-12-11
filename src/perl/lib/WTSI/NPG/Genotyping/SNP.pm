
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
  (is       => 'ro',
   isa      => SNPSet,
   required => 1,
   weak_ref => 1);

sub is_gender_marker {
  my ($self) = @_;

  return $self->name =~ m{^GS};
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
