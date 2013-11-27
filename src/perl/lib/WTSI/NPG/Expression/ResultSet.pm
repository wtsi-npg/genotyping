
use utf8;

package WTSI::NPG::Expression::ResultSet;

use Moose;

with 'WTSI::NPG::Loggable';

has 'sample_id'        => (is => 'ro', isa => 'Str', required => 1);
has 'plate_id'         => (is => 'ro', isa => 'Str', required => 0);
has 'well_id'          => (is => 'ro', isa => 'Str', required => 0);

has 'beadchip'         => (is => 'ro', isa => 'Str', required => 1);
has 'beadchip_section' => (is => 'ro', isa => 'Str', required => 1);

has 'xml_file'         => (is => 'ro', isa => 'Str', required => 1);
has 'idat_file'        => (is => 'ro', isa => 'Str', required => 1);

sub BUILD {
  my ($self) = @_;

  unless (-e $self->xml_file) {
    $self->logconfess("The XML file '", $self->xml_file,
                      "' does not exist");
  }

  unless (-e $self->idat_file) {
    $self->logconfess("The idat file '", $self->idat_file,
                      "' does not exist");
  }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
