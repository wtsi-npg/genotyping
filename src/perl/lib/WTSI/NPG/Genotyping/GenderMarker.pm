
use utf8;

package WTSI::NPG::Genotyping::GenderMarker;

use Moose;

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'name' => (is => 'ro', isa => 'Str', required => 1);

has 'x_marker' =>
  (is       => 'ro',
   isa      => XMarker,
   required => 1);

has 'y_marker' =>
  (is       => 'ro',
   isa      => YMarker,
   required => 1);

sub BUILD {
  my ($self) = @_;

  my $x_snpset = $self->x_marker->snpset;
  my $y_snpset = $self->y_marker->snpset;

  unless ((defined $x_snpset && defined $y_snpset) &&
          ($x_snpset == $y_snpset)) {
    my $x_snpset_str = defined $x_snpset ? $x_snpset->str : 'undef';
    my $y_snpset_str = defined $y_snpset ? $y_snpset->str : 'undef';

    $self->logconfess("A GenderMarker may be constructed only from SNPs from ",
                      "the same SNPSet: X SNP ",
                      $self->x_marker->str, " from ", $x_snpset_str,
                      ", Y SNP ",
                      $self->y_marker->str, " from ", $y_snpset_str);
  }
}

sub chromosome {
  my ($self) = @_;

  return $self->x_marker->chromosome;
}

sub strand {
  my ($self) = @_;

  return $self->x_marker->strand;
}

sub position {
  my ($self) = @_;

  return $self->x_marker->position;
}

sub ref_allele {
  my ($self) = @_;

  return $self->x_marker->ref_allele;
}

sub alt_allele {
  my ($self) = @_;

  return $self->y_marker->ref_allele;
}

sub snpset {
  my ($self) = @_;

  return $self->x_marker->snpset;
}

sub equals {
  my ($self, $other) = @_;

  return ($other->isa(__PACKAGE__)                  &&
          $self->x_marker->equals($other->x_marker) &&
          $self->y_marker->equals($other->y_marker));
}

sub str {
  my ($self) = @_;

  return join "\t", $self->name, $self->ref_allele, $self->alt_allele,
    $self->chromosome, $self->position, $self->strand;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::GenderMarker - Information on a single gender marker

=head1 SYNOPSIS

   my $marker = WTSI::NPG::Genotyping::GenderMarker->new(name     => $name,
                                                         x_marker => $snp1,
                                                         y_marker => $snp2)

=head1 DESCRIPTION

A instance of GenderMarker represents a gender marker on a
specific reference (or references). These markers are used to determine
the gender of a sample. Each maps to 2 loci, one on the X chromosome and
one on the Y chromosome. When treated as a "SNP" during genotyping,
females being XX will report only one allele, while males being XY will
report both.

When reporting locus position, this class explicitly returns values for
the X marker only.

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
