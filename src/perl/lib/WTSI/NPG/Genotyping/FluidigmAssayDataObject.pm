
package WTSI::NPG::Genotyping::FluidigmAssayDataObject;

use Moose;

use WTSI::NPG::iRODS qw(get_object_meta);
use WTSI::NPG::Genotyping::Metadata qw($FLUIDIGM_PLATE_NAME_META_KEY
                                       $FLUIDIGM_PLATE_WELL_META_KEY);
use WTSI::NPG::Metadata qw(make_sample_metadata);
use WTSI::NPG::Publication qw(expected_irods_groups
                              grant_group_access);

extends 'WTSI::NPG::iRODS::Path';

sub update_secondary_metadata {
  my ($self, $ssdb) = @_;

  my (undef, $fluidigm_barcode) = $self->get_avu($FLUIDIGM_PLATE_NAME_META_KEY);
  my (undef, $well) = $self->get_avu($FLUIDIGM_PLATE_WELL_META_KEY);

  $self->debug("Found plate well '$fluidigm_barcode': '$well' in ",
               "current metadata of '", $self->str, "'");

  my $ss_sample =
    $ssdb->find_fluidigm_sample_by_plate($fluidigm_barcode, $well);

  unless ($ss_sample) {
    $self->logconfess("Failed to update metadata for '", $self->str,
                      "': failed to find sample in '$fluidigm_barcode' ",
                      "well '$well'");
  }

  $self->info("Updating metadata for '", $self->str ,"' from plate ",
              "'$fluidigm_barcode' well '$well'");

  my @meta = make_sample_metadata($ss_sample);
  foreach my $avu (@meta) {
    $self->add_avu(@$avu);
  }

  my @groups = expected_irods_groups(@meta);
  grant_group_access($self->str, 'read', @groups);

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;


