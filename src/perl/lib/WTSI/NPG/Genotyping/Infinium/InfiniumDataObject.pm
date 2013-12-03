
use utf8;

package WTSI::NPG::Genotyping::Infinium::InfiniumDataObject;

use Moose;

use WTSI::NPG::Genotyping::Metadata qw($INFINIUM_PLATE_BARCODE_META_KEY
                                       $INFINIUM_PLATE_WELL_META_KEY);
use WTSI::NPG::Metadata qw(make_sample_metadata);

extends 'WTSI::NPG::iRODS::DataObject';

sub update_secondary_metadata {
  my ($self, $ssdb) = @_;

  my $infinium_barcode_avu = $self->get_avu($INFINIUM_PLATE_BARCODE_META_KEY);
  my $infinium_barcode = $infinium_barcode_avu->{value};
  my $well_avu = $self->get_avu($INFINIUM_PLATE_WELL_META_KEY);
  my $well = $well_avu->{value};

  $self->debug("Found plate well '$infinium_barcode': '$well' in ",
               "current metadata of '", $self->str, "'");

  my $ss_sample =
    $ssdb->find_infinium_sample_by_plate($infinium_barcode, $well);

  if ($ss_sample) {
    $self->info("Updating metadata for '", $self->str, "' from plate ",
                "'$infinium_barcode' well '$well'");

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
                   "'$infinium_barcode' well '$well'");
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
