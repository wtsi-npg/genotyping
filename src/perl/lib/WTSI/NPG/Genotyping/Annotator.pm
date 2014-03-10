use utf8;

package WTSI::NPG::Genotyping::Annotator;

use Moose::Role;
use UUID;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Genotyping::Annotation';

=head2 make_infinium_metadata

  Arg [1]    : sample hashref from WTSI::NPG::Genotyping::Database::Infinium
  Example    : my @meta = $obj->make_infinium_metadata($sample)
  Description: Return a list of metadata key/value pairs describing the
               sample in the Infinium LIMS. Includes the beadchip identifier.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_infinium_metadata {
  my ($self, $if_sample) = @_;

  return ([$self->infinium_beadchip_attr         => $if_sample->{beadchip}],
          [$self->infinium_beadchip_section_attr => $if_sample->{beadchip_section}],
          [$self->infinium_beadchip_design_attr  => $if_sample->{beadchip_design}],
          [$self->infinium_project_title_attr    => $if_sample->{project}],
          [$self->infinium_sample_name_attr      => $if_sample->{sample}],
          [$self->infinium_plate_barcode_attr    => $if_sample->{plate}],
          [$self->infinium_plate_well_attr       => $if_sample->{well}]);
}

sub make_sequenom_metadata {
  my ($self, $well) = @_;

  return ([$self->sequenom_plate_name_attr => $well->{plate}],
          [$self->sequenom_plate_well_attr => $well->{well}]);
}

sub make_fluidigm_metadata {
  my ($self, $well) = @_;

  return ([$self->fluidigm_plate_name_attr => $well->{plate}],
          [$self->fluidigm_plate_well_attr => $well->{well}]);
}

=head2 make_analysis_metadata

  Arg [1]    : Arrayref of genotyping project titles
  Example    : my @meta = $obj->make_analysis_metadata(\@titles)
  Description: Return a list of metadata key/value pairs describing an analysis
               including the genotyping project names involved.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_analysis_metadata {
  my ($self, $genotyping_project_titles) = @_;

  my $uuid_bin;
  my $uuid_str;
  UUID::generate($uuid_bin);
  UUID::unparse($uuid_bin, $uuid_str);

  my @meta = ([$self->analysis_uuid_attr => $uuid_str]);

  foreach my $title (@$genotyping_project_titles) {
    push(@meta, [$self->infinium_project_title_attr => $title]);
  }

  return @meta;
}

sub infinium_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$self->infinium_beadchip_attr,
                                  $self->infinium_beadchip_section_attr,
                                  # $self->infinium_beadchip_design_attr,
                                  $self->infinium_project_title_attr,
                                  $self->infinium_sample_name_attr,
                                  $self->infinium_plate_barcode_attr,
                                  $self->infinium_plate_well_attr],
                                 \@meta);
}

sub sequenom_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$self->sequenom_plate_name_attr,
                                  $self->sequenom_plate_well_attr],
                                 \@meta);
}

sub fluidigm_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$self->fluidigm_plate_name_attr,
                                  $self->fluidigm_plate_well_attr],
                                 \@meta);
}

no Moose;

1;

__END__

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
