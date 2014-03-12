use utf8;

package WTSI::NPG::Genotyping::Annotation;

use Moose::Role;

our %GENOTYPING_METADATA_ATTR =
  (analysis_uuid             => 'analysis_uuid',
   infinium_project_title    => 'dcterms:title',
   infinium_beadchip         => 'beadchip',
   infinium_beadchip_design  => 'beadchip_design',
   infinium_beadchip_section => 'beadchip_section',
   infinium_plate_name       => 'infinium_plate',
   infinium_plate_well       => 'infinium_well',
   infinium_sample_name      => 'infinium_sample',
   sequenom_plate_name       => 'sequenom_plate',
   sequenom_plate_well       => 'sequenom_well',
   sequenom_plex_name        => 'sequenom_plex',
   fluidigm_plate_name       => 'fluidigm_plate',
   fluidigm_plate_well       => 'fluidigm_well',
   fluidigm_plex_name        => 'fluidigm_plex');

my $meta = __PACKAGE__->meta;

foreach my $attr_name (keys %GENOTYPING_METADATA_ATTR) {
  my %options = (is       => 'ro',
                 isa      => 'Str',
                 required => 1,
                 default  => $GENOTYPING_METADATA_ATTR{$attr_name});

  $meta->add_attribute($attr_name . '_attr', %options);
}

no Moose;

1;

__END__

=head1 NAME

Annotation - Metadata attribute names.

=head1 DESCRIPTION

Provides methods to access metadata attribute names.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
