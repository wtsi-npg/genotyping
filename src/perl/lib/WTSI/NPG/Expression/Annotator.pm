use utf8;

package WTSI::NPG::Expression::Annotator;

use List::AllUtils qw(any);
use Moose::Role;
use UUID;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Expression::Annotation';

our @VALID_PROFILE_GROUPINGS = qw(group sample);
our @VALID_SUMMARY_TYPES     = qw(gene probe annotation);

=head2 make_infinium_metadata

  Arg [1]    : sample hashref
  Example    : my @meta = $obj->make_infinium_metadata($sample)
  Description: Return a list of metadata key/value pairs describing the
               sample. Includes the beadchip identifier.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_infinium_metadata {
  my ($self, $resultset) = @_;

  my @meta =
    ([$self->dcterms_identifier_attr          => $resultset->sample_id],
     [$self->expression_beadchip_attr         => $resultset->beadchip],
     [$self->expression_beadchip_section_attr => $resultset->beadchip_section]);

  if ($resultset->plate_id) {
    push @meta,  [$self->expression_plate_name_attr => $resultset->plate_id];
  }
  if ($resultset->well_id) {
    push @meta,  [$self->expression_plate_well_attr => $resultset->well_id];
  }

  return @meta;
}

sub make_profile_metadata {
   my ($self, $normalisation_method, $grouping, $type) = @_;

   any { $grouping eq $_ } @VALID_PROFILE_GROUPINGS
     or $self->logconfess("Invalid profile grouping '$grouping'");
   any { $type eq $_ } @VALID_SUMMARY_TYPES
     or $self->logconfess("Invalid summary type '$type'");

   my @meta =
     ([$self->expression_summary_group_attr => $grouping],
      [$self->expression_summary_type_attr  => $type],
      [$self->expression_norm_method_attr   => $normalisation_method]);

   return @meta;
}

sub make_profile_annotation_metadata {
  my ($self, $type) = @_;

  any { $type eq $_ } @VALID_SUMMARY_TYPES
    or $self->logconfess("Invalid summary type '$type'");

  return ([$self->expression_summary_type_attr => $type]);
}

=head2 make_analysis_metadata

  Arg [1]    : UUID to use instead of gereating a new one. Optional.
  Example    : my @meta = $obj->make_analysis_metadata()
  Description: Return a list of metadata key/value pairs describing an analysis.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_analysis_metadata {
  my ($self, $supplied_uuid) = @_;

  my $uuid_str;
  if (defined $supplied_uuid) {
    $uuid_str = $supplied_uuid;
  }
  else {
    my $uuid_bin;
    UUID::generate($uuid_bin);
    UUID::unparse($uuid_bin, $uuid_str);
  }

  my @meta = ([$self->analysis_uuid_attr => $uuid_str]);

  return @meta;
}

sub infinium_fingerprint {
  my ($self, @meta) = @_;

  return $self->make_fingerprint([$self->expression_beadchip_attr,
                                  $self->expression_beadchip_section_attr],
                                 \@meta);
}

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
