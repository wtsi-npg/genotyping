
package WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;

use Data::Dump qw(dump);
use Moose;
use Try::Tiny;

use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;

our $VERSION = '';

with 'WTSI::NPG::Annotator', 'WTSI::NPG::Genotyping::Annotator';

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
  my ($self, $whdb) = @_;

  my $fluidigm_barcode;
  my $well;

  my $fluidigm_barcode_avu = $self->get_avu($self->fluidigm_plate_name_attr);
  if ($fluidigm_barcode_avu) {
    $fluidigm_barcode = $fluidigm_barcode_avu->{value};
  }

  my $well_avu = $self->get_avu($self->fluidigm_plate_well_attr);
  if ($well_avu) {
    $well = $well_avu->{value};
  }

  unless ($fluidigm_barcode) {
    $self->logcarp("Failed updata metadata for '", $self->str,
                   "': failed to find an Fluidigm barcode in the existing ",
                   "metadata");
  }
  unless ($well) {
    $self->logcarp("Failed updata metadata for '", $self->str,
                   "': failed to find a well address in the existing ",
                   "metadata");
  }

  $self->debug("Found plate well '$fluidigm_barcode': '$well' in ",
               "current metadata of '", $self->str, "'");

  my $wh_sample =
    $whdb->find_fluidigm_sample_by_plate($fluidigm_barcode, $well);

  if ($wh_sample) {
    $self->info("Updating metadata for '", $self->str, "' from plate ",
                "'$fluidigm_barcode' well '$well'");

    # Supersede all the secondary metadata with new values
    my @meta = $self->make_sample_metadata($wh_sample);
    # Sorting by attribute to allow repeated updates to be in
    # deterministic order
    @meta = sort { $a->[0] cmp $b->[0] } @meta;

    $self->debug("Superseding AVUs in order of attributes: [",
                 join(q{, }, map { $_->[0] } @meta), "]");

    foreach my $avu (@meta) {
      try {
        $self->supersede_avus(@$avu);
      } catch {
        $self->error("Failed to supersede with AVU ", dump($avu), ": ", $_);
      };
    }

    $self->update_group_permissions;
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

Copyright (c) 2013, 2014, 2015 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
