
use utf8;

package WTSI::NPG::Expression::InfiniumDataObject;

use Data::Dump qw(dump);
use Moose;
use Try::Tiny;

use WTSI::NPG::iRODS::Metadata; # has attribute name constants

our $VERSION = '';

extends 'WTSI::NPG::iRODS::DataObject';

with 'WTSI::NPG::Annotator', 'WTSI::NPG::Expression::Annotator';

sub update_secondary_metadata {
  my ($self, $ssdb) = @_;

  my $plate;
  my $well;
  my $sample_id;

  my $plate_avu = $self->get_avu($self->expression_plate_name_attr);
  if ($plate_avu) {
    $plate = $plate_avu->{value};
  }

  my $well_avu = $self->get_avu($self->expression_plate_well_attr);
  if ($well_avu) {
    $well = $well_avu->{value};
  }

  my $sample_id_avu = $self->get_avu($DCTERMS_IDENTIFIER);
  if ($sample_id_avu) {
    $sample_id = $sample_id_avu->{value};
  }

  my $ss_sample;

  if ($plate && $well) {
    # Using a V2 manifest, which has plate tracking information
    $ss_sample = $ssdb->find_infinium_gex_sample($plate, $well);
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

    # Supersede all the secondary metadata with new values
    my @meta = $self->make_sample_metadata($ss_sample);
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
    $self->logcroak("Failed to update metadata for '", $self->str,
                    "': failed to find in the warehouse a sample ",
                    "'$sample_id'");
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

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
