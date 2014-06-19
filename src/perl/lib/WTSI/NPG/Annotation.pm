use utf8;

package WTSI::NPG::Annotation;

use Moose::Role;

our %METADATA_ATTR =
  (sample_name             => 'sample',
   sample_id               => 'sample_id',
   sample_supplier_name    => 'sample_supplier_name',
   sample_common_name      => 'sample_common_name',
   sample_accession_number => 'sample_accession_number',
   sample_cohort           => 'sample_cohort',
   sample_control          => 'sample_control',
   sample_consent          => 'sample_consent',
   sample_donor_id         => 'sample_donor_id',
   study_id                => 'study_id',
   study_title             => 'study_title',
   reference_genome_name   => 'reference_name',
   file_type               => 'type',
   file_md5                => 'md5',
   rt_ticket               => 'rt_ticket',
   dcterms_audience        => 'dcterms:audience',
   dcterms_creator         => 'dcterms:creator',
   dcterms_created         => 'dcterms:created',
   dcterms_identifier      => 'dcterms:identifier',
   dcterms_modified        => 'dcterms:modified',
   dcterms_publisher       => 'dcterms:publisher',
   dcterms_title           => 'dcterms:title');

my $meta = __PACKAGE__->meta;

foreach my $attr_name (keys %METADATA_ATTR) {
  my %options = (is       => 'ro',
                 isa      => 'Str',
                 required => 1,
                 default  => $METADATA_ATTR{$attr_name});

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
