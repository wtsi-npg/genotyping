
use utf8;

package WTSI::NPG::Genotyping::GenderMarkerCall;

use Moose;

extends 'WTSI::NPG::Genotyping::Call';

use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

# extension of Call class for gender markers
# adds a gender attribute and is_male, is_female methods

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

sub is_x_call {
    my ($self) = @_;
    if ($self->gender == $FEMALE_GENDER) { return 1; }
    else { return 0; }
}

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
