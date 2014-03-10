
use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayDataObject;

use Moose;

with 'WTSI::NPG::Annotator', 'WTSI::NPG::Genotyping::Annotator';

extends 'WTSI::NPG::iRODS::DataObject';

sub update_secondary_metadata {
  my ($self, $snpdb, $ssdb) = @_;

  my $sequenom_plate_avu = $self->get_avu($self->sequenom_plate_name_attr);
  my $plate_name = $sequenom_plate_avu->{value};
  my $well_avu = $self->get_avu($self->sequenom_plate_well_attr);
  my $well = $well_avu->{value};

  $self->debug("Found plate well '$plate_name': '$well' in ",
               "current metadata of '", $self->str, "'");

  # Identify the plate via the SNP database.  It would be preferable
  # to look up directly in the warehouse.  However, the warehouse does
  # not contain tracking information on Sequenom plates
  my $plate_id = $snpdb->find_sequenom_plate_id($plate_name);
  if (defined $plate_id) {
    $self->debug("Found Sequencescape plate identifier '$plate_id' for '",
                 $self->str, "'");

    my $ss_sample = $ssdb->find_sample_by_plate($plate_id, $well);
    unless ($ss_sample) {
      $self->logconfess("Failed to update metadata for '", $self->str, "': ",
                        "failed to find in the warehouse a sample in ",
                        "'$plate_name' (ID $plate_id) well '$well'");
    }

    $self->info("Updating metadata for '", $self->str, "' from plate ",
                "'$plate_name' (ID $plate_id) well '$well'");

    # Revoke access from current groups
    my @current_groups = $self->expected_irods_groups;
    $self->set_permissions('null', @current_groups);

    # Supersede all the secondary metadata with new values
    my @meta = $self->make_sample_metadata($ss_sample);

    foreach my $avu (@meta) {
      $self->supersede_avus(@$avu);
    }

    # Grant access to the new groups
    my @groups = $self->expected_irods_groups;
    $self->set_permissions('read', @groups);
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

__END__

=head1 NAME

WTSI::NPG::Genotyping::Sequenom::AssayDataObject

=head1 SYNOPSIS

  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
    ($irods, "/irods_root/0123456789/0123456789_A01.csv");

=head1 DESCRIPTION

A class which represents to result of a Sequenom assay of one sample
as an iRODS data object.

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
