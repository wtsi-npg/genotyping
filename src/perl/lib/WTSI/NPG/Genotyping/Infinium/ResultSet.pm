
use utf8;

package WTSI::NPG::Genotyping::Infinium::ResultSet;

use Moose;

use WTSI::NPG::Genotyping::Types qw(InfiniumBeadchipBarcode
                                    InfiniumBeadchipSection);

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

has 'beadchip'         => (
    is => 'ro',
    isa => InfiniumBeadchipBarcode,
    required => 1);
has 'beadchip_section' => (
    is => 'ro',
    isa => InfiniumBeadchipSection,
    required => 1);

has 'beadchip_design'  => (is => 'ro', isa => 'Str',  required => 1);
has 'gtc_file'         => (is => 'ro', isa => 'Str',  required => 0);
has 'red_idat_file'    => (is => 'ro', isa => 'Str',  required => 1);
has 'grn_idat_file'    => (is => 'ro', isa => 'Str',  required => 1);
has 'is_methylation'   => (is => 'ro', isa => 'Bool', required => 0);

sub BUILD {
  my ($self) = @_;

  if ($self->is_methylation && $self->gtc_file) {
    $self->logconfess("A methylation result set cannot contain a GTC file: '",
                      $self->gtc_file, "'");
  }

  if ($self->gtc_file) {
    unless (-e $self->gtc_file) {
      $self->logconfess("The GTC file '", $self->gtc_file, "' does not exist");
    }
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

sub size {
  my ($self) = @_;

  return $self->is_methylation ? 2 : 3;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__


=head1 NAME

WTSI::NPG::Genotyping::Infinium::ResultSet

=head1 DESCRIPTION

A class which represents the result files of an Infinium genotyping
array assay of one sample.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
