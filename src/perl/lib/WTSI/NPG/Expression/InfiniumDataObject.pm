
use utf8;

package WTSI::NPG::Expression::InfiniumDataObject;

use Moose;

use WTSI::NPG::Metadata qw(make_sample_metadata);

extends 'WTSI::NPG::iRODS::DataObject';

sub update_secondary_metadata {
  my ($self, $ssdb, $sample_id, $plate_barcode, $well) = @_;

  my $ss_sample;

  if ($plate_barcode && $well) {
    # Using a V2 manifest, which has plate tracking information
    $ss_sample = $ssdb->find_infinium_gex_sample($plate_barcode, $well);
    my $expected_sanger_id = $ss_sample->{sanger_sample_id};

    unless ($sample_id eq $expected_sanger_id) {
      $self->logconfess("Sample in plate '$plate_barcode' well '$well' ",
                        "has an incorrect Sanger sample ID '$sample_id' ",
                        "(expected '$expected_sanger_id'");
    }
  }
  else {
    # Using a V1 manifest, which does not have plate tracking information
    $ss_sample = $ssdb->find_infinium_gex_sample_by_sanger_id($sample_id);
    $self->warn("Plate tracking information is absent for sample '$sample_id'",
                "; using its Sanger sample ID instead");
  }

  if ($ss_sample) {
    $self->info("Updating metadata for '", $self->str, "' from plate '",
                $ss_sample->{barcode}, "' well '", $ss_sample->{map}, "'");

    my @meta = make_sample_metadata($ss_sample);
    foreach my $avu (@meta) {
      $self->add_avu(@$avu);
    }

    my @groups = $self->expected_irods_groups;
    $self->grant_group_access('read', @groups);
  }
  else {
    $self->logcarp("Failed to update metadata for '", $self->str,
                   "': failed to find in the warehouse a sample ",
                   "'$sample_id'");
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
