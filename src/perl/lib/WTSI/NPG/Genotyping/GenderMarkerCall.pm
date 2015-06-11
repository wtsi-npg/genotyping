
use utf8;

package WTSI::NPG::Genotyping::GenderMarkerCall;

use Moose;

extends 'WTSI::NPG::Genotyping::Call';

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'snp' =>
  (is       => 'ro',
   isa      => GenderMarker,
   required => 1);

has 'gender' =>
    (is      => 'ro',
     isa     => 'Int',
     lazy    => 1,
     builder => '_build_gender');

our $UNKNOWN_GENDER = 0;
our $FEMALE_GENDER = 1;
our $MALE_GENDER = 2;

sub BUILD {
  my ($self) = @_;
  if ($self->is_heterozygous()) {
      $self->logcroak("Gender marker cannot have a heterozygous genotype!");
  }
}

=head2 complement

  Arg [1]    : None

  Example    : my $new_gm_call = $gm_call->complement
  Description: Return a new call object whose genotype is complemented
               with respect to the original, retaining qscore (if any).
               Overrides method in the parent class to return a
               GenderMarkerCall.
  Returntype : WTSI::NPG::Genotyping::GenderMarkerCall

=cut

sub complement {
  my ($self) = @_;
  my $complement_genotype = $self->_complement($self->genotype);
  if (defined($self->qscore)) {
      return WTSI::NPG::Genotyping::GenderMarkerCall->new
          (snp      => $self->snp,
           genotype => $self->_complement($self->genotype),
           qscore   => $self->qscore,
           is_call  => $self->is_call);
  } else {
      return WTSI::NPG::Genotyping::GenderMarkerCall->new
          (snp      => $self->snp,
           genotype => $self->_complement($self->genotype),
           is_call  => $self->is_call);
  }
}

=head2 is_x_call

  Arg [1]    : None

  Example    : $is_x = $gender_marker_call->is_x_call()
  Description: Determine whether the called genotype is an X chromosome call
               for this gender marker. Returns true for a call on the X
               chromosome, false for a call on the Y chromosome or no call.
               IMPORTANT: This only evaluates a single gender marker. It is
               not to be confused with evaluating the gender status of a
               given sample (which typically uses multiple markers).
  Returntype : Bool

=cut

sub is_x_call {
    my ($self) = @_;
    if ($self->gender == $FEMALE_GENDER) { return 1; }
    else { return 0; }
}

=head2 is_y_call

  Arg [1]    : None

  Example    : $is_y = $gender_marker_call->is_y_call()
  Description: Determine whether the called genotype is a Y chromosome call
               for this gender marker. Returns true for a call on the Y
               chromosome, false for a call on the X chromosome or no call.
               IMPORTANT: This only evaluates a single gender marker. It is
               not to be confused with evaluating the gender status of a
               given sample (which typically uses multiple markers).
  Returntype : Bool

=cut

sub is_y_call {
    my ($self) = @_;
    if ($self->gender == $MALE_GENDER) { return 1; }
    else { return 0; }
}

sub _build_gender {
    # find the gender, taking into account forward/reverse strand
    my ($self) = @_;
    my $gt;
    if ($self->snp->strand() eq '-') { # marker is on the reverse strand
        $gt = $self->complement()->genotype();
    } else {
        $gt = $self->genotype();
    }
    my $base = substr($gt, 0, 1);
    my $gender;
    if ($base eq $self->snp->x_marker->ref_allele) {
        $gender = $FEMALE_GENDER;
    } elsif ($base eq $self->snp->y_marker->ref_allele) {
        $gender = $MALE_GENDER;
    } elsif (!$self->is_call()) {
        $gender = $UNKNOWN_GENDER;
    } else {
        $self->logcroak("Called genotype '", $self->genotype() ,
                        "' is inconsistent with strand ",
                        "and X/Y alleles of gender marker '",
                        $self->snp->name(), "'");
    }
    return $gender;
}


__END__

=head1 NAME

WTSI::NPG::Genotyping::GenderMarkerCall

=head1 DESCRIPTION

Extension of the WTSI::NPG::Genotyping::Call class to handle gender markers.
The input variant is required to be of the GenderMarker type. Includes methods
to determine whether the call is consistent with an X (female) or Y (male)
variant.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
