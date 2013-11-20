
use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayDataObject;

use Moose;

use WTSI::NPG::Genotyping::Metadata qw($SEQUENOM_PLATE_NAME_META_KEY
                                       $SEQUENOM_PLATE_WELL_META_KEY);
use WTSI::NPG::Metadata qw(make_sample_metadata);

extends 'WTSI::NPG::iRODS::DataObject';

sub update_secondary_metadata {
  my ($self, $snpdb, $ssdb) = @_;

  my $sequenom_plate_avu = $self->get_avu($SEQUENOM_PLATE_NAME_META_KEY);
  my $plate_name = $sequenom_plate_avu->{value};
  my $well_avu = $self->get_avu($SEQUENOM_PLATE_WELL_META_KEY);
  my $well = $well_avu->{value};

  $self->debug("Found plate well '$plate_name': '$well' in ",
               "current metadata of '", $self->str, "'");

  # Identify the plate via the SNP database.  It would be preferable
  # to look up directly in the warehouse.  However, the warehouse does
  # not contain tracking information on Sequenom plates
  my $plate_id = $snpdb->find_sequenom_plate_id($plate_name);
  if (defined $plate_id) {
    $self->debug("Found Sequencescape plate identifier '$plate_id' for ",
                 "'", $self->str, "'");

    my $ss_sample = $ssdb->find_sample_by_plate($plate_id, $well);
    unless ($ss_sample) {
      $self->logconfess("Failed to update metadata for '", $self->str, "': ",
                        "failed to find in the warehouse a sample in ",
                        "'$plate_name' (ID $plate_id) well '$well'");
    }

    $self->info("Updating metadata for '", $self->str, "' from plate ",
                "'$plate_name' (ID $plate_id) well '$well'");

    my @meta = make_sample_metadata($ss_sample);
    foreach my $avu (@meta) {
      $self->add_avu(@$avu);
    }

    my @groups = $self->expected_irods_groups;
    $self->grant_group_access('read', @groups);
  }
  else {
    $self->info("Skipping update of metadata for '", $self->str, "': ",
                "plate name '$plate_name' is not present in SNP database");
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
