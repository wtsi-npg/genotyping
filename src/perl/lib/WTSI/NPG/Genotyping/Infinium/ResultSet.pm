
use utf8;

package WTSI::NPG::Genotyping::Infinium::ResultSet;

use Moose;

with 'WTSI::NPG::Loggable';

has 'beadchip'         => (is => 'ro', isa => 'Str', required => 1);
has 'beadchip_section' => (is => 'ro', isa => 'Str', required => 1);

has 'gtc_file'         => (is => 'ro', isa => 'Str', required => 1);
has 'red_idat_file'    => (is => 'ro', isa => 'Str', required => 1);
has 'grn_idat_file'    => (is => 'ro', isa => 'Str', required => 1);

sub BUILD {
  my ($self) = @_;

  unless ($self->beadchip =~ m{^\d{10}$}) {
    $self->logconfess("Invalid beadchip number '", $self->beadchip, "'");
  }

  unless ($self->beadchip_section =~ m{^R\d+C\d+$}) {
    $self->logconfess("Invalid beadchip section '", $self->beadchip_section,
                      "'");
  }

  unless (-e $self->gtc_file) {
    $self->logconfess("The GTC file '", $self->gtc_file,
                      "' does not exist");
  }

  unless (-e $self->grn_idat_file) {
    $self->logconfess("The Grn idat file '", $self->grn_idat_file,
                      "' does not exist");
  }

  unless (-e $self->red_idat_file) {
    $self->logconfess("The Red idat file '", $self->red_idat_file,
                      "' does not exist");
  }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
