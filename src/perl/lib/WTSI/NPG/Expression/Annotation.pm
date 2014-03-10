use utf8;

package WTSI::NPG::Expression::Annotation;

use Moose::Role;

our %EXPRESSION_METADATA_ATTR =
  (analysis_uuid               => 'analysis_uuid',
   expression_project_title    => 'dcterms:title',
   expression_beadchip         => 'beadchip',
   expression_beadchip_design  => 'beadchip_design',
   expression_beadchip_section => 'beadchip_section',
   expression_plate_name       => 'gex_plate',
   expression_plate_well       => 'gex_well');

my $meta = __PACKAGE__->meta;

foreach my $attr_name (keys %EXPRESSION_METADATA_ATTR) {
  my %options = (is       => 'ro',
                 isa      => 'Str',
                 required => 1,
                 default  => $EXPRESSION_METADATA_ATTR{$attr_name});

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
