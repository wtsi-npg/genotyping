
use utf8;

package WTSI::NPG::Genotyping::GenderMarkerCall;

use Moose;

extends 'WTSI::NPG::Genotyping::Call';

use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

our $NULL_GENOTYPE = 'NN';
our $UNKNOWN_GENDER = 0;
our $FEMALE_GENDER = 1;
our $MALE_GENDER = 2;

with 'WTSI::DNAP::Utilities::Loggable';

has 'snp' =>
  (is       => 'ro',
   isa      => GenderMarker,
   required => 1);

has '_gender' =>
    (is      => 'ro',
     isa     => 'Int',
     lazy    => 1,
     builder => '_build_gender',
     init_arg => undef);

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

=head2 is_female

  Arg [1]    : None

  Example    : $is_f = $gender_marker_call->is_female()
  Description: Determine whether the called genotype is an X homozygote,
               corresponding to a female sample.
               IMPORTANT: This only evaluates a single gender marker. It is
               not to be confused with evaluating the gender status of a
               given sample (which typically uses multiple markers).
  Returntype : Bool

=cut

sub is_female {
    my ($self) = @_;
    if ($self->_gender == $FEMALE_GENDER) { return 1; }
    else { return 0; }
}

=head2 is_male

  Arg [1]    : None

  Example    : $is_m = $gender_marker_call->is_male()
  Description: Determine whether the called genotype is an X/Y heterozygote,
               corresponding to a male sample.
               IMPORTANT: This only evaluates a single gender marker. It is
               not to be confused with evaluating the gender status of a
               given sample (which typically uses multiple markers).
  Returntype : Bool

=cut

sub is_male {
    my ($self) = @_;
    if ($self->_gender == $MALE_GENDER) { return 1; }
    else { return 0; }
}


=head2 xy_call_pair

  Arg [1]    : None

  Example    : $call_pair = $gender_marker_call->xy_call_pair()
  Description: Return Call objects for the X and Y markers belonging to
               this GenderMarker. A male sample will have calls for both
               X and Y; a female will have a call for X and no-call for Y.
               A sample of unknown gender will return two no-calls.
  Returntype : ArrayRef[WTSI::NPG::Genotyping::Call]

=cut

sub xy_call_pair {
    my ($self) = @_;
    my @calls;
    my $x_base = $self->snp->x_allele();
    my $y_base = $self->snp->y_allele();
    if ($self->is_female()) {
        push(@calls, WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->x_marker,
            genotype => $x_base.$x_base,
            qscore   => $self->qscore,
        ));
        push(@calls, WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->y_marker,
            genotype => $NULL_GENOTYPE,
            is_call  => 0,
        ));
    } elsif ($self->is_male()) {
        push(@calls, WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->x_marker,
            genotype => $x_base.$x_base,
            qscore   => $self->qscore,
        ));
        push(@calls, WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->y_marker,
            genotype => $y_base.$y_base,
            qscore   => $self->qscore,
        ));
    } else {
        push(@calls, WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->x_marker,
            genotype => $NULL_GENOTYPE,
            is_call  => 0,
        ));
        push(@calls, WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->y_marker,
            genotype => $NULL_GENOTYPE,
            is_call  => 0,
        ));
    }
    if ($self->snp->strand eq '-') {
        my @complemented_calls;
        foreach my $call (@calls) {
            push @complemented_calls, $call->complement();
        }
        return \@complemented_calls;
    } else {
        return \@calls;
    }
}


sub _build_gender {
    # find the gender, taking into account forward/reverse strand
    my ($self) = @_;
    my $gender;
    if (!$self->is_call()) {
        $gender = $UNKNOWN_GENDER;
    } elsif ($self->is_homozygous() || $self->is_homozygous_complement) {
        $gender = $FEMALE_GENDER;
    } elsif ($self->is_heterozygous() || $self->is_heterozygous_complement) {
        $gender = $MALE_GENDER;
    } else {
        $self->logcroak("Called genotype '", $self->genotype() ,
                        "' is inconsistent with gender marker '",
                        $self->snp->name(), "'");
    }
    return $gender;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

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
