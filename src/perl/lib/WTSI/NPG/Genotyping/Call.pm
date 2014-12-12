
use utf8;

package WTSI::NPG::Genotyping::Call;

use Moose;

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'snp' =>
  (is       => 'ro',
   isa      => Variant,
   required => 1);

has 'genotype' =>
  (is       => 'ro',
   isa      => SNPGenotype,
   required => 1);

has 'is_call' =>
  (is       => 'rw',
   isa      => 'Bool',
   default  => 1); # used to represent 'no calls'

sub BUILD {
  my ($self) = @_;
  if ($self->genotype eq 'NN') {
    $self->is_call(0);
  }

  if ($self->is_call) {
    my $gt  = $self->genotype;
    my $snp = $self->snp;
    my $r = $snp->ref_allele;
    my $a = $snp->alt_allele;

    if ($self->is_complement) {
      unless ($self->is_homozygous_complement ||
              $self->is_heterozygous_complement) {
        $self->logconfess("The complement genotype '$gt' is not possible ",
                          "for SNP ", $snp->name, " which has ref allele '$r' ",
                          "and alt allele '$a'");
      }
    }
    else {
      unless ($self->is_homozygous || $self->is_heterozygous) {
        $self->logconfess("The genotype '$gt' is not possible ",
                          "for SNP ", $snp->name, " which has ref allele '$r' ",
                          "and alt allele '$a'");
      }
    }
  }
}

sub is_homozygous {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $rr = $r . $r;
  my $aa = $a . $a;
  my $gt = $self->genotype;

  return $gt eq $rr || $gt eq $aa;
}

sub is_heterozygous {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $ra = $r . $a;
  my $ar = $a . $r;
  my $gt = $self->genotype;

  return $gt eq $ra || $gt eq $ar;
}

sub is_homozygous_complement {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $rr = $r . $r;
  my $aa = $a . $a;
  my $cgt = _complement($self->genotype);

  return $cgt eq $rr || $cgt eq $aa;
}

sub is_heterozygous_complement {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $ra = $r . $a;
  my $ar = $a . $r;
  my $cgt = _complement($self->genotype);

  return $cgt eq $ra || $cgt eq $ar;
}

sub is_complement {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $rr = $r . $r; # Homozygous ref
  my $aa = $a . $a; # Homozygous alt
  my $ra = $r . $a; # Heterozygous
  my $ar = $a . $r; # Heterozygous
  my $cgt = _complement($self->genotype);

  return $cgt eq $rr || $cgt eq $aa || $cgt eq $ra || $cgt eq $ar;
}

sub complement {
  my ($self) = @_;

  return WTSI::NPG::Genotyping::Call->new
    (snp      => $self->snp,
     genotype => _complement($self->genotype),
     is_call  => $self->is_call);
}

=head2 merge

  Arg [1]    : WTSI::NPG::Genotyping::Call

  Example    : $new_call = $call->merge($other_call)
  Description: Merge results of this call with another on the same SNP:
               - If the genotypes are identical, return $self unchanged.
               - If exactly one of the two calls is a 'no call', return the
               non-null call.
               - If two non-null genotypes are in conflict, die with error
  Returntype : WTSI::NPG::Genotyping::Call

=cut

sub merge {
    my ($self, $other) = @_;
    unless ($self->snp->equals($other->snp)) {
        $self->logconfess("Attempted to merge calls for non-identical SNPs");
    }
    my $merged;
    if ($self->is_call && !($other->is_call)) {
        $merged = $self;
    } elsif (!($self->is_call) && $other->is_call) {
        $merged = $other;
    } elsif ($self->genotype eq $other->genotype) {
        $merged = $self;
    } else {
        $self->logdie("Unable to merge differing non-null genotype calls ",
                      "for SNP '", $self->snp->name, "': '",
                      $self->genotype, "', '", $other->genotype, "'");
    }
    return $merged;
}

sub _complement {
  my ($genotype) = @_;

  $genotype =~ tr/ACGTNacgtn/TGCANtgcan/;
  return $genotype;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>
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
