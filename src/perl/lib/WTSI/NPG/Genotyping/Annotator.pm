use utf8;

package WTSI::NPG::Genotyping::Annotator;

use Moose::Role;
use UUID;

use WTSI::NPG::iRODS::Metadata; # has attribute name constants

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

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

  return
    ([$INFINIUM_BEADCHIP         => $if_sample->{beadchip}],
     [$INFINIUM_BEADCHIP_SECTION => $if_sample->{beadchip_section}],
     [$INFINIUM_BEADCHIP_DESIGN  => $if_sample->{beadchip_design}],
     [$INFINIUM_PROJECT_TITLE    => $if_sample->{project}],
     [$INFINIUM_SAMPLE_NAME      => $if_sample->{sample}],
     [$INFINIUM_PLATE_NAME       => $if_sample->{plate}],
     [$INFINIUM_PLATE_WELL       => $if_sample->{well}]);
}

sub make_sequenom_metadata {
  my ($self, $well) = @_;

  return ([$SEQUENOM_PLATE_NAME => $well->{plate}],
          [$SEQUENOM_PLATE_WELL => $well->{well}]);
}

sub make_fluidigm_metadata {
  my ($self, $well) = @_;

  return ([$FLUIDIGM_PLATE_NAME => $well->{plate}],
          [$FLUIDIGM_PLATE_WELL => $well->{well}]);
}

sub make_manual_qc_metadata {
  my ($self, $manual_qc) = @_;

  return ([$QC_STATE => $manual_qc]);
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

  my @meta = ([$ANALYSIS_UUID => $uuid_str]);

  foreach my $title (@$genotyping_project_titles) {
    push(@meta, [$INFINIUM_PROJECT_TITLE => $title]);
  }

  return @meta;
}

sub infinium_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$INFINIUM_BEADCHIP,
                                  $INFINIUM_BEADCHIP_SECTION,
                                  $INFINIUM_BEADCHIP_DESIGN,
                                  $INFINIUM_PROJECT_TITLE,
                                  $INFINIUM_SAMPLE_NAME,
                                  $INFINIUM_PLATE_NAME,
                                  $INFINIUM_PLATE_WELL],
                                 \@meta);
}

sub sequenom_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$SEQUENOM_PLATE_NAME,
                                  $SEQUENOM_PLATE_WELL],
                                 \@meta);
}

sub fluidigm_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$FLUIDIGM_PLATE_NAME,
                                  $FLUIDIGM_PLATE_WELL],
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
