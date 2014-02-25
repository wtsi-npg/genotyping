
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;

use Moose;

use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::Metadata qw($FLUIDIGM_PLATE_NAME_META_KEY
                                       $FLUIDIGM_PLATE_WELL_META_KEY);
use WTSI::NPG::Metadata qw(make_sample_metadata);

extends 'WTSI::NPG::iRODS::DataObject';


=head2 assay_resultset

  Arg [1]    : None

  Example    : $resultset = $data_object->assay_resultset;
  Description: Return a parsed ResultSet from the content of this iRODS
               data object.
  Returntype : WTSI::NPG::Genotyping::Fluidigm::AssayResultSet

=cut

sub assay_resultset {
  my ($self) = @_;

  return WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new($self);
}

sub update_secondary_metadata {
  my ($self, $ssdb) = @_;

  my $fluidigm_barcode_avu = $self->get_avu($FLUIDIGM_PLATE_NAME_META_KEY);
  my $fluidigm_barcode = $fluidigm_barcode_avu->{value};
  my $well_avu = $self->get_avu($FLUIDIGM_PLATE_WELL_META_KEY);
  my $well = $well_avu->{value};

  $self->debug("Found plate well '$fluidigm_barcode': '$well' in ",
               "current metadata of '", $self->str, "'");

  my $ss_sample =
    $ssdb->find_fluidigm_sample_by_plate($fluidigm_barcode, $well);

  if ($ss_sample) {
    $self->info("Updating metadata for '", $self->str, "' from plate ",
                "'$fluidigm_barcode' well '$well'");

    # Revoke access from current groups
    my @current_groups = $self->expected_irods_groups;
    $self->set_permissions('null', @current_groups);

    # Supersede all the secondary metadata with new values
    my @meta = make_sample_metadata($ss_sample);
    foreach my $avu (@meta) {
      $self->supersede_avus(@$avu);
    }

    # Grant access to the new groups
    my @groups = $self->expected_irods_groups;
    $self->set_permissions('read', @groups);
  }
  else {
    $self->logcarp("Failed to update metadata for '", $self->str,
                   "': failed to find in the warehouse a sample in ",
                   "'$fluidigm_barcode' well '$well'");
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::AssayDataObject

=head1 SYNOPSIS

  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "/irods_root/1381735059/S01_1381735059.csv");

=head1 DESCRIPTION

A class which represents the result of a Fluidigm assay of one sample
as an iRODS data object. This contains the raw data results for a
number of SNPs.

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
