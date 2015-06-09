
use utf8;

package WTSI::NPG::Genotyping::Call;

use Moose;

use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

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

=head2 is_homozygous

  Arg [1]    : None

  Example    : $call->is_homozygous
  Description: Return true if the call is homozygous i.e. both alleles
               are identical to either the reference or alt alleles.
  Returntype : Bool

=cut

sub is_homozygous {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $rr = $r . $r;
  my $aa = $a . $a;
  my $gt = $self->genotype;

  return $gt eq $rr || $gt eq $aa;
}

=head2 is_heterozygous

  Arg [1]    : None

  Example    : $call->is_heterozygous
  Description: Return true if the call is heterozygous i.e. one allele
               is identical to the reference and the other identical to the
               alt allele.
  Returntype : Bool

=cut

sub is_heterozygous {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $ra = $r . $a;
  my $ar = $a . $r;
  my $gt = $self->genotype;

  return $gt eq $ra || $gt eq $ar;
}

=head2 is_homozygous_complement

  Arg [1]    : None

  Example    : $call->is_homozygous_complement
  Description: Return true if the call is homozygous i.e. both alleles
               are identical to the complement of either the reference or
               alt alleles.
  Returntype : Bool

=cut

sub is_homozygous_complement {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $rr = $r . $r;
  my $aa = $a . $a;
  my $cgt = _complement($self->genotype);

  return $cgt eq $rr || $cgt eq $aa;
}

=head2 is_heterozygous_complement

  Arg [1]    : None

  Example    : $call->is_heterozygous_complement
  Description: Return true if the call is heterozygous i.e. one allele
               is identical to the complement of the reference and the
               other identical to the complement of the alt allele.
  Returntype : Bool

=cut

sub is_heterozygous_complement {
  my ($self) = @_;

  my $r = $self->snp->ref_allele;
  my $a = $self->snp->alt_allele;

  my $ra = $r . $a;
  my $ar = $a . $r;
  my $cgt = _complement($self->genotype);

  return $cgt eq $ra || $cgt eq $ar;
}

=head2 is_complement

  Arg [1]    : None

  Example    : $call->is_complement
  Description: Return true if the call is homozygous or heterozygous when
               compared to the complement of the reference and alt alleles.
  Returntype : Bool

=cut

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

=head2 complement

  Arg [1]    : None

  Example    : my $new_call = $call->complement
  Description: Return a new call object whose genotype is complemented
               with respect to the original.
  Returntype : WTSI::NPG::Genotyping::Call

=cut

sub complement {
  my ($self) = @_;

  return WTSI::NPG::Genotyping::Call->new
    (snp      => $self->snp,
     genotype => _complement($self->genotype),
     is_call  => $self->is_call);
}

=head2 merge

  Arg [1]    : WTSI::NPG::Genotyping::Call

  Example    : my $new_call = $call->merge($other_call)
  Description: Merge results of this call with another on the same SNP:
               - If the genotypes are identical, return $self unchanged.
               - If exactly one of the two calls is a 'no call', return the
               non-null call.
               - If two non-null genotypes are in conflict, die with error
  Returntype : WTSI::NPG::Genotyping::Call

=cut

sub merge {
  my ($self, $other) = @_;

  defined $other or
    $self->logconfess("A defined other argument is required");

  $self->snp->equals($other->snp) or
    $self->logconfess("Attempted to merge calls for non-identical SNPs: ",
                      $self->snp->name, " and ", $other->snp->name);

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

=head2 equivalent

  Arg [1]    : WTSI::NPG::Genotyping::Call

  Example    : my $equiv = $call->equivalent($other_call)
  Description: Compare two calls on the same SNP and return true if they are
               equivalent. Calls with the ref and alt alleles swapped and/or
               complemented are considered equivalent. If one or both calls
               are a no-calls, they are considered non-equivalent.
  Returntype : Bool

=cut

sub equivalent {
  my ($self, $other) = @_;

  defined $other or
    $self->logconfess("A defined other argument is required");

  $self->snp->equals($other->snp) or
    $self->logconfess("Attempted to compare calls for non-identical SNPs: ",
                      $self->snp->name, " and ", $other->snp->name);

  my $equivalent = 0;

  if ($self->is_call && $other->is_call) {
    if ($self->genotype eq $other->genotype) {
      $self->debug($self->genotype, " is equivalent to ", $other->genotype,
                   " for ", $self->snp->name);
      $equivalent = 1;
    }
    elsif ((scalar reverse $self->genotype) eq $other->genotype) {
      $self->debug("Reverse of ", $self->genotype, " is equivalent to ",
                   $other->genotype, " for ", $self->snp->name);
      $equivalent = 1;
    }
    else {
      my $complement = $self->complement;

      if ($complement->genotype eq $other->genotype) {
        $self->debug("Complement of ", $self->genotype,
                     " is equivalent to ", $other->genotype, " for ",
                     $self->snp->name);
        $equivalent = 1;
      }
      elsif ((scalar reverse $complement->genotype) eq $other->genotype) {
        $self->debug("Reverse complement of ", $self->genotype,
                     " is equivalent to ", $other->genotype);
        $equivalent = 1;
      }
      else {
        $self->debug($self->genotype, " is not equivalent to ",
                     $other->genotype, " for ", $self->snp->name);
      }
    }
  }
  else  {
    my $sc = $self->is_call  ? 'call' : 'no call';
    my $oc = $other->is_call ? 'call' : 'no call';

    $self->debug($self->genotype, " ($sc) is not equivalent to ",
                 $other->genotype, " ($oc) for ", $self->snp->name);
  }

  return $equivalent;
}

sub str {
  my ($self) = @_;

  return sprintf("%s call:%s SNP: %s",
                 $self->genotype,
                 $self->is_call ? 'yes' : 'no',
                 $self->snp->str);
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
