
use utf8;

package WTSI::NPG::Genotyping::GenderMarkerCall;

use Moose;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::GenderMarker;

extends 'WTSI::NPG::Genotyping::Call';

use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

our $NULL_GENOTYPE = 'NN';
our $UNKNOWN_GENDER = 0;
our $FEMALE_GENDER = 1;
our $MALE_GENDER = 2;

with 'WTSI::DNAP::Utilities::Loggable';

has 'snp' =>
  (is        => 'ro',
   isa       => GenderMarker,
   lazy      => 1,
   builder   => '_build_snp'
);

has 'genotype' =>
  (is        => 'ro',
   isa       => SNPGenotype,
   lazy      => 1,
   builder   => '_build_genotype'
);

has 'x_call' =>
  (is        => 'ro',
   isa       => XMarkerCall,
   lazy      => 1,
   builder   => '_build_x_call'
);

has 'y_call' =>
  (is       => 'ro',
   isa      => YMarkerCall,
   lazy      => 1,
   builder   => '_build_y_call'
);

has '_gender' =>
    (is      => 'ro',
     isa     => 'Int',
     lazy    => 1,
     builder => '_build_gender',
     init_arg => undef);

# unlike parent class, SNP and genotype are not required arguments
# must supply EITHER snp, genotype OR x_call, y_call

sub BUILD {
    my ($self, $args) = @_;
    my $valid = 0;
    if (defined($args->{'x_call'}) && defined($args->{'y_call'})) {
        if (!(defined($args->{'genotype'}) || defined($args->{'snp'}))) {
            $valid = 1;
        }
    } elsif (defined($args->{'genotype'}) && defined($args->{'snp'})) {
        if (!(defined($args->{'x_call'}) || defined($args->{'y_call'}))) {
            $valid = 1;
        }
    }
    # die if arguments are invalid
    unless ($valid) {
        $self->logconfess( "Invalid arguments to GenderMarkerCall ",
                           "constructor: Must supply either snp and ",
                           "genotype, or x_call and y_call, but not both");
    }
    # now check validity of genotype (input, or derived from x/y calls)
    $self->_validate_genotype(); # method of parent class
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

sub _build_genotype {
    # find genotype from X and Y call inputs
    my ($self) = @_;
    my $genotype;
    my $is_call = 1;
    if (!($self->x_call->is_call)) { # no call
        $is_call = 0;
        $genotype = $NULL_GENOTYPE;
    } elsif ($self->y_call->is_call) { # male call
        my $x_allele = substr($self->x_call->genotype, 0, 1);
        my $y_allele = substr($self->y_call->genotype, 0, 1);
        $genotype = $x_allele.$y_allele;
    } else { # female call
        $genotype = $self->x_call->genotype;
    }
    return $genotype;
}

sub _build_snp {
    my ($self) = @_;
    if ($self->x_call->snp->name ne $self->y_call->snp->name) {
        $self->logconfess("Mismatched SNP names: X = '",
                          $self->x_call->snp->name, "', Y = '",
                          $self->y_call->snp->name, "'"
                      );
    }
    my $x_strand = $self->x_call->snp->strand;
    my $y_strand = $self->y_call->snp->strand;
    if ($x_strand ne $y_strand) {
        $self->logconfess("Mismatched strand directions for input ",
                          "X and Y markers");
    }
    my %args = (
        name     => $self->x_call->snp->name,
        x_marker => $self->x_call->snp,
        y_marker => $self->y_call->snp,
    );
    if (defined($x_strand)) { $args{'strand'} = $x_strand; }
    return WTSI::NPG::Genotyping::GenderMarker->new(%args);
}

sub _build_x_call {
    my ($self) = @_;
    my $x_call;
    my $x_base = $self->snp->x_allele();
    if ($self->is_female() || $self->is_male()) {
        $x_call = WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->x_marker,
            genotype => $x_base.$x_base,
            qscore   => $self->qscore,
        );
    } else {
        $x_call = WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->x_marker,
            genotype => $NULL_GENOTYPE,
            is_call  => 0,
        );
    }
    if ($self->is_complement) { $x_call = $x_call->complement; }
    return $x_call;
}

sub _build_y_call {
    my ($self) = @_;
    my $y_call;
    my $y_base = $self->snp->y_allele();
    if ($self->is_male()) {
        $y_call = WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->y_marker,
            genotype => $y_base.$y_base,
            qscore   => $self->qscore,
        );
    } else {
        $y_call = WTSI::NPG::Genotyping::Call->new(
            snp      => $self->snp->y_marker,
            genotype => $NULL_GENOTYPE,
            is_call  => 0,
        );
    }
    if ($self->is_complement) { $y_call = $y_call->complement; }
    return $y_call;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::GenderMarkerCall

=head1 DESCRIPTION

Extension of the WTSI::NPG::Genotyping::Call class to handle gender markers.

=head2 Motivation

In genotyping, a gender marker consists of two SNP variants, one each on
the X and Y chromosome. The X variant is expected to be homozygous in
females. The two variants together are used to evaluate whether a sample is
male or female.

There are two ways of representing a call on a gender marker:
1) Joined: Record the gender marker name and a concatenated genotype, which
will be homozygous for a female sample and heterozygous for a male sample.
In the latter case, the major and minor alleles are respectively from the
X and Y variants.
2) Split: Record the X and Y variants and their genotypes separately. The
variants have the same name, but different chromosome and position.

Format 1) is used in results from Sequenom/Fluidigm, and in the Plink
output from the genotyping pipeline. Format 2) is required by VCF, in which
each variant must have a single chromosome and position.

=head2 Use cases: 'Split' and 'Join' operations

In order to write Sequenom/Fluidigm calls as VCF, we convert from joined to
split format in WTSI::NPG::Genotyping::VCF::AssayResultParser. This is done
by simply using the x_call and y_call attributes of a GenderMarkerCall.

In order to read VCF genotypes and compare with Plink output for the
identity check, we convert from split to joined format in
WTSI::NPG::Genotyping::VCF::VCFDataSet. This is done by collating the X and
Y marker calls read from VCF input, then using the X and Y calls on a given
GenderMarker to construct a GenderMarkerCall.


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
