use utf8;

package WTSI::NPG::Expression::ChipLoadingManifest;

use Moose;

with 'WTSI::NPG::Loggable';

has 'file_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'samples' =>
  (is       => 'rw',
   isa      => 'ArrayRef',
   required => 1,
   default  => sub { return [] });

sub _validate_sample_id {
  my ($self, $sample_id, $line) = @_;

  unless (defined $sample_id) {
    $self->logcroak("Missing sample ID at line $line\n");
  }

  unless ($sample_id =~ m{^\S+$}) {
    $self->logcroak("Invalid sample ID '$sample_id' at line $line\n");
  }

  return $sample_id;
}

sub _validate_plate_id {
  my ($self, $plate_id, $line) = @_;

  unless (defined $plate_id) {
    $self->logcroak("Missing Supplier Plate ID at line $line\n");
  }

  unless ($plate_id =~ m{^\S+$}) {
    $self->logcroak("Invalid Supplier plate ID '$plate_id' at line $line\n");
  }

  return $plate_id;
}

sub _validate_well_id {
  my ($self, $well_id, $line) = @_;

  unless (defined $well_id) {
    $self->logcroak("Missing Supplier well ID at line $line\n");
  }

  my ($row, $column) = $well_id =~ m{^([A-H])([1-9]+[0-2]?)$};
  unless ($row && $column) {
    $self->logcroak("Invalid Supplier well ID '$well_id' at line $line\n");
  }
  unless ($column >= 1 && $column <= 12) {
    $self->logcroak("Invalid Supplier well ID '$well_id' at line $line\n");
  }

  return $well_id;
}

sub _validate_beadchip {
  my ($self, $chip, $line) = @_;

  unless (defined $chip) {
    $self->logcroak("Missing beadchip number at line $line\n");
  }

  unless ($chip =~ m{^\d{10}$}) {
    $self->logcroak("Invalid beadchip number '$chip' at line $line\n");
  }

  return $chip;
}

sub _validate_section {
  my ($self, $section, $line) = @_;

  unless (defined $section) {
    $self->logcroak("Missing beadchip section at line $line\n");
  }

  unless ($section =~ m{^[A-Z]$}) {
    $self->logcroak("Invalid beadchip section '$section' at line $line\n");
  }

  return $section;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;


