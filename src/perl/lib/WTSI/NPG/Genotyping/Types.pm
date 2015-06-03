
use utf8;

package WTSI::NPG::Genotyping::Types;

use MooseX::Types::Moose qw(ArrayRef Str Int);

use MooseX::Types -declare =>
  [
   qw(
      ArrayRefOfReference
      ArrayRefOfVariant
      DNABase
      DNAStrand
      GenderMarker
      HsapiensAutosome
      HsapiensChromosome
      HsapiensChromosomeVCF
      HsapiensHeterosome
      HsapiensMT
      HsapiensX
      HsapiensY
      Platform
      Reference
      SNP
      SNPGenotype
      SNPSet
      Variant
      XMarker
      YMarker
    )
  ];

subtype HsapiensChromosome,
  as Str,
  where { $_ =~ m{(^[Cc]hr)?[\d+|MT|X|Y]$} },
  message { "'$_' is not a valid H. sapiens chromosome name" };

subtype HsapiensChromosomeVCF,
  as Str,
  where { $_ =~ m{^([0-9]+|[MT|X|Y]{1})$} },
  message { "'$_' is not a valid H. sapiens chromosome name for VCF" };

subtype HsapiensX,
  as Str,
  where { $_ =~ m{(^[Cc]hr)?X$} },
  message { "'$_' is not a valid H. sapiens X chromosome" };

subtype HsapiensY,
  as Str,
  where { $_ =~ m{(^[Cc]hr)?Y$} },
  message { "'$_' is not a valid H. sapiens Y chromosome" };

subtype DNABase,
  as Str,
  where { $_ =~ m{^[ACGTNacgtn]$} },
  message { "'$_' is not a valid DNA base" };

subtype DNAStrand,
  as Str,
  where { $_ eq '+' || $_ eq '-' },
  message { "'$_' is not a valid DNA strand" };

# A genotype call represented as a 2 allele string.
subtype SNPGenotype,
  as Str,
  where { length($_) == 2              &&
          is_DNABase(substr($_, 0, 1)) &&
          is_DNABase(substr($_, 1, 1)) },
  message { "'$_' is not a valid SNP call"};

subtype Platform,
  as Str,
  where { $_ eq 'fluidigm' || $_ eq 'sequenom' },
  message { "'$_' is not a valid genotyping platform" };

class_type GenderMarker, { class => 'WTSI::NPG::Genotyping::GenderMarker' };
class_type Reference,    { class => 'WTSI::NPG::Genotyping::Reference' };
class_type SNP,          { class => 'WTSI::NPG::Genotyping::SNP' };
class_type SNPSet,       { class => 'WTSI::NPG::Genotyping::SNPSet' };

subtype Variant,
  as GenderMarker | SNP;

subtype XMarker,
  as SNP,
  where { is_HsapiensX($_->chromosome) },
  message { $_->name . ' on ' . $_->chromosome .
              ' is not a valid X chromosome marker' };

subtype YMarker,
  as SNP,
  where { is_HsapiensY($_->chromosome) },
  message { $_->name . ' on ' . $_->chromosome .
              ' is not a valid Y chromosome marker' };

subtype ArrayRefOfReference,
  as ArrayRef[Reference];

subtype ArrayRefOfVariant,
  as ArrayRef[Variant];

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Types - Moose types for genotyping

=head1 DESCRIPTION

The non-core Moose types for genotyping are all defined here.

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
