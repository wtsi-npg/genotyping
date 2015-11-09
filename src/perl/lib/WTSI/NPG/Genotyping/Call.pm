
use utf8;

package WTSI::NPG::Genotyping::Call;

use Moose;

use WTSI::NPG::Genotyping::GenderMarker;
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

has 'is_call'    =>
  (is            => 'rw',
   isa           => 'Bool',
   default       => 1,
   documentation => "used to represent 'no calls'");

has 'qscore'     =>
  (is            => 'ro',
   isa           => 'Maybe['.QualityScore.']',
   documentation => "May be a Phred quality score (positive integer),".
       " or undef if the score is missing or not defined");

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


=head2 clone

  Arg [1]    : None

  Example    : $cloned_call = $call->clone()
  Description: Return an identical copy of the call.
  Returntype : WTSI::NPG::Genotyping::Call

=cut

# used to generate test data for evaluating the Bayesian identity check
# (some test code is external to WTSI genotyping pipeline)

sub clone {
  my ($self) = @_;
  my %args = (snp => $self->snp,
              genotype => $self->genotype,
              is_call => $self->is_call,
          );
  if (defined $self->qscore) {
      $args{'qscore'} = $self->qscore;
  }
  return WTSI::NPG::Genotyping::Call->new(\%args);
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
  my $cgt = $self->_complement($self->genotype);

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
  my $cgt = $self->_complement($self->genotype);

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
  my $cgt = $self->_complement($self->genotype);

  return $cgt eq $rr || $cgt eq $aa || $cgt eq $ra || $cgt eq $ar;
}

=head2 complement

  Arg [1]    : None

  Example    : my $new_call = $call->complement
  Description: Return a new call object whose genotype is complemented
               with respect to the original, retaining qscore (if any).
  Returntype : WTSI::NPG::Genotyping::Call

=cut

sub complement {
  my ($self) = @_;
  if (defined($self->qscore)) {
      return WTSI::NPG::Genotyping::Call->new
          (snp      => $self->snp,
           genotype => $self->_complement($self->genotype),
           qscore   => $self->qscore,
           is_call  => $self->is_call);
  } else {
      return WTSI::NPG::Genotyping::Call->new
          (snp      => $self->snp,
           genotype => $self->_complement($self->genotype),
           is_call  => $self->is_call);
  }
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


=head2 merge_x_y_markers

  Arg [1]    : WTSI::NPG::Genotyping::Call

  Example    : my $new_call = $x_call->merge_x_y_markers($y_call), OR:
               my $new_call = $y_call->merge_x_y_markers($x_call)
  Description: Merge a call on the Y chromosome with a call on the X
               chromosome (or vice versa) to create a new call with a
               GenderMarker instead of a SNP as its variant. The input calls
               must be an XMarker and YMarker with the same SNP name.
               Acceptable input genotypes are:
               - X hom, Y hom: Male
               - X hom, Y no-call: Female
               - X no-call, Y no-call: No-call
               Any other combination of input genotypes will result in a
               no-call with a warning.
  Returntype : WTSI::NPG::Genotyping::Call

=cut

sub merge_x_y_markers {
    my ($self, $other) = @_;
    my ($x_call, $y_call);
    if ($self->snp->is_XMarker) { $x_call = $self; }
    elsif ($self->snp->is_YMarker) { $y_call = $self; }
    if ($other->snp->is_XMarker) { $x_call = $other; }
    elsif ($other->snp->is_YMarker) { $y_call = $other; }
    unless ($x_call && $y_call) {
        $self->logconfess("Can only use merge_gender to merge an X call ",
                          "with a Y call or vice versa. Input SNPs were: '",
                          $self->snp->name, "', '", $other->snp->name, "'",
                          "', on respective chromosomes: ",
                          $self->snp->chromosome, ", ",
                          $other->snp->chromosome);
    }
    unless ($x_call->snp->name eq $y_call->snp->name) {
        $self->logconfess("Cannot merge X and Y markers with differing ",
                          "names: '", $x_call->snp->name, "', '",
                          $y_call->snp->name, "'");
    }
    my $gender_marker = WTSI::NPG::Genotyping::GenderMarker->new(
        name     => $x_call->snp->name,
        x_marker => $x_call->snp,
        y_marker => $y_call->snp
    );
    my $genotype;
    my $is_call = 0;
    my $x_hom = $x_call->is_homozygous || $x_call->is_homozygous_complement;
    my $y_hom = $y_call->is_homozygous || $y_call->is_homozygous_complement;
    if ($x_call->is_call && $x_hom) {
        if (!($y_call->is_call)) { # female
            $genotype = $x_call->genotype;
            $is_call = 1;
        } elsif ($y_hom) { # male
            my $x_allele = substr($x_call->genotype, 0, 1);
            my $y_allele = substr($y_call->genotype, 0, 1);
            $genotype = $x_allele.$y_allele;
            $is_call = 1;
        }
    } elsif (!$x_call->is_call && !$y_call->is_call) { # no data
        $genotype = 'NN';
    }
    unless ($genotype) {
        $self->logwarn("Cannot construct genotype for merged ",
                       "gender marker call, with calls: '",
                       $x_call->str, "', '",
                       $y_call->str, "'; assigning no call.");
        $genotype = 'NN';
    }
    return WTSI::NPG::Genotyping::Call->new(
        snp      => $gender_marker,
        genotype => $genotype,
        is_call  => $is_call
    );
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
  # If $self is a GenderMarkerCall, $self->snp->equals will use the equals()
  # method of the GenderMarker class. So two GenderMarkerCalls are equal
  # iff their GenderMarker attributes are equal.

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
  my ($self, $genotype) = @_;

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
