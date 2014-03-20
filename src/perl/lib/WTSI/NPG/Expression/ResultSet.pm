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

__END__

=head1 NAME

WTSI::NPG::Expression::ResultSet

=head1 DESCRIPTION

A class which represents the result files of an Infinium gene
expression array assay of one sample.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
