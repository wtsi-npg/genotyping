use utf8;

package WTSI::NPG::Expression::ChipLoadingManifest;

use Moose;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

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

  unless ($sample_id =~ m{^\S+$}msx) {
    $self->logcroak("Invalid sample ID '$sample_id' at line $line\n");
  }

  return $sample_id;
}

sub _validate_plate_id {
  my ($self, $plate_id, $line) = @_;

  unless (defined $plate_id) {
    $self->logcroak("Missing Supplier Plate ID at line $line\n");
  }

  unless ($plate_id =~ m{^\S+$}msx) {
    $self->logcroak("Invalid Supplier plate ID '$plate_id' at line $line\n");
  }

  return $plate_id;
}

sub _validate_well_id {
  my ($self, $well_id, $line) = @_;

  unless (defined $well_id) {
    $self->logcroak("Missing Supplier well ID at line $line\n");
  }

  my ($row, $column) = $well_id =~ m{^([A-H])([1-9]+[0-2]?)$}msx;
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

  unless ($chip =~ m{^\d{10}$}msx) {
    $self->logcroak("Invalid beadchip number '$chip' at line $line\n");
  }

  return $chip;
}

sub _validate_section {
  my ($self, $section, $line) = @_;

  unless (defined $section) {
    $self->logcroak("Missing beadchip section at line $line\n");
  }

  unless ($section =~ m{^[[:upper:]]$}msx) {
    $self->logcroak("Invalid beadchip section '$section' at line $line\n");
  }

  return $section;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
