
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;

use Moose;

use WTSI::NPG::Genotyping::Metadata qw($FLUIDIGM_PLATE_NAME_META_KEY
                                       $FLUIDIGM_PLATE_WELL_META_KEY);
use WTSI::NPG::Metadata qw(make_sample_metadata);

extends 'WTSI::NPG::iRODS::DataObject';

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
    $self->info("Updating metadata for '", $self->str ,"' from plate ",
                "'$fluidigm_barcode' well '$well'");

    my @meta = make_sample_metadata($ss_sample);
    foreach my $avu (@meta) {
      $self->add_avu(@$avu);
    }

    my @groups = $self->expected_irods_groups;
    $self->grant_group_access('read', @groups);
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
