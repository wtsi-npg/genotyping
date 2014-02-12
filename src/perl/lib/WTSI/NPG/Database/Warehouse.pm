
use utf8;

package WTSI::NPG::Database::Warehouse;

use Carp;
use Moose;

use WTSI::NPG::Utilities qw(depad_map);

extends 'WTSI::NPG::Database';

=head2 find_plate

  Arg [1]    : string
  Example    : $db->find_plate($plate_id)
  Description: Return plate details for a plate identifier
               as a hashref with plate addresses as keys and values being
               a further hashref for each sample having the following keys
               and values:
               { internal_id,      => <SequenceScape plate id>,
                 sanger_sample_id  => <WTSI sample name string>,
                 consent_withdrawn => <boolean, true if now unconsented>,
                 uuid              => <SequenceScape UUID blob as hexidecimal>,
                 name              => <SequenceScape sample name>,
                 common_name       => <SequenceScape sample common name>,
                 supplier_name     => <Supplier provided name, may be undef>,
                 gender            => <Supplier gender string>,
                 cohort            => <Supplier cohort string>,
                 control           => <Supplier control flag>,
                 study_id          => <SequenceScape study id>,
                 barcode_prefix    => <SequenceScape barcode prefix string>,
                 barcode           => <SequenceScape barcode integer>,
                 map               => <SequenceScape well address string
                                       without 0-pad e.g A1> }
  Returntype : hashref
  Caller     : general

=cut

sub find_plate {
  my ($self, $plate_id) = @_;

  defined $plate_id or $self->logconfess('The plate_id argument was undefined');

  my $query =
    qq(SELECT
         sm.internal_id,
         sm.sanger_sample_id,
         sm.consent_withdrawn,
         HEX(sm.uuid),
         sm.name,
         sm.common_name,
         sm.supplier_name,
         sm.accession_number,
         sm.gender,
         sm.cohort,
         sm.control,
         aq.study_internal_id AS study_id,
         pl.barcode_prefix,
         pl.barcode,
         pl.plate_purpose_name,
         wl.map
       FROM
         current_plates pl,
         current_samples sm,
         current_wells wl,
         current_aliquots aq
       WHERE
         pl.internal_id = ?
         AND wl.plate_barcode = pl.barcode
         AND wl.plate_barcode_prefix = pl.barcode_prefix
         AND wl.internal_id = aq.receptacle_internal_id
         AND aq.sample_internal_id = sm.internal_id);

  $self->trace("Executing: '$query' with args [$plate_id]");

  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_id);

  my %plate;
  while (my $row = $sth->fetchrow_hashref) {
    $plate{$row->{map}} = $row;
  }

  return \%plate;
}

sub find_sample_by_plate {
  my ($self, $plate_id, $map) = @_;

  defined $plate_id or $self->logconfess('The plate_id argument was undefined');
  defined $map or $self->logconfess('The map argument was undefined');

  my $unpadded_map = depad_map($map);

  my $query =
    qq(SELECT
         sm.internal_id,
         sm.sanger_sample_id,
         sm.consent_withdrawn,
         HEX(sm.uuid),
         sm.name,
         sm.common_name,
         sm.supplier_name,
         sm.accession_number,
         sm.gender,
         sm.cohort,
         sm.control,
         aq.study_internal_id AS study_id,
         pl.barcode_prefix,
         pl.barcode,
         pl.plate_purpose_name,
         wl.map
       FROM
         current_plates pl,
         current_samples sm,
         current_wells wl,
         current_aliquots aq
       WHERE
         pl.internal_id = ?
         AND wl.plate_barcode = pl.barcode
         AND wl.plate_barcode_prefix = pl.barcode_prefix
         AND wl.map = ?
         AND wl.internal_id = aq.receptacle_internal_id
         AND aq.sample_internal_id = sm.internal_id);

  $self->trace("Executing: '$query' with args ",
               "[$plate_id, $unpadded_map]");

  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_id, $unpadded_map);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n samples were returned where 1 sample was expected");
  }

  return shift @samples;
}

=head2 find_infinium_plate

  Arg [1]    : string
  Example    : $db->find_infinium_plate('Infinium LIMS plate barcode')
  Description: Return plate details for an Infinium LIMS plate barcode
               as a hashref with plate addresses as keys and values being
               a further hashref for each sample having the following keys
               and values:
               { internal_id,      => <SequenceScape plate id>,
                 sanger_sample_id  => <WTSI sample name string>,
                 consent_withdrawn => <boolean, true if now unconsented>,
                 uuid              => <SequenceScape UUID blob as hexidecimal>,
                 name              => <SequenceScape sample name>,
                 common_name       => <SequenceScape sample common name>,
                 supplier_name     => <Supplier provided name, may be undef>,
                 gender            => <Supplier gender string>,
                 cohort            => <Supplier cohort string>,
                 control           => <Supplier control flag>,
                 study_id          => <SequenceScape study id>,
                 barcode_prefix    => <SequenceScape barcode prefix string>,
                 barcode           => <SequenceScape barcode integer>,
                 map               => <SequenceScape well address string
                                       without 0-pad e.g A1> }
  Returntype : hashref
  Caller     : general

=cut

sub find_infinium_plate {
  my ($self, $infinium_barcode) = @_;

  defined $infinium_barcode or
    $self->logconfess('The infinium_barcode argument was undefined');

  my $query =
    qq(SELECT
         sm.internal_id,
         sm.sanger_sample_id,
         sm.consent_withdrawn,
         HEX(sm.uuid),
         sm.name,
         sm.common_name,
         sm.supplier_name,
         sm.accession_number,
         sm.gender,
         sm.cohort,
         sm.control,
         aq.study_internal_id AS study_id,
         pl.barcode_prefix,
         pl.barcode,
         pl.plate_purpose_name,
         wl.map
       FROM
         current_plates pl,
         current_samples sm,
         current_wells wl,
         current_aliquots aq
       WHERE
         pl.infinium_barcode = ?
         AND wl.plate_barcode = pl.barcode
         AND wl.plate_barcode_prefix = pl.barcode_prefix
         AND wl.internal_id = aq.receptacle_internal_id
         AND aq.sample_internal_id = sm.internal_id);

  $self->trace("Executing: '$query' with args [$infinium_barcode]");

  my $sth = $self->dbh->prepare($query);
  $sth->execute($infinium_barcode);

  my %plate;
  while (my $row = $sth->fetchrow_hashref) {
    $plate{$row->{map}} = $row;
  }

  return \%plate;
}

sub find_infinium_sample_by_plate {
  my ($self, $infinium_barcode, $map) = @_;

  defined $infinium_barcode or
    $self->logconfess('The infinium_barcode argument was undefined');
  defined $map or $self->logconfess('The map argument was undefined');

  my $unpadded_map = depad_map($map);

  my $query =
    qq(SELECT
         sm.internal_id,
         sm.sanger_sample_id,
         sm.consent_withdrawn,
         HEX(sm.uuid),
         sm.name,
         sm.common_name,
         sm.supplier_name,
         sm.accession_number,
         sm.gender,
         sm.cohort,
         sm.control,
         aq.study_internal_id AS study_id,
         pl.barcode_prefix,
         pl.barcode,
         pl.plate_purpose_name,
         wl.map
       FROM
         current_plates pl,
         current_samples sm,
         current_wells wl,
         current_aliquots aq
       WHERE
         pl.infinium_barcode = ?
         AND wl.plate_barcode = pl.barcode
         AND wl.plate_barcode_prefix = pl.barcode_prefix
         AND wl.map = ?
         AND wl.internal_id = aq.receptacle_internal_id
         AND aq.sample_internal_id = sm.internal_id);

  $self->trace("Executing: '$query' with args ",
               "[$infinium_barcode, $unpadded_map]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($infinium_barcode, $unpadded_map);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n samples were returned where 1 sample was expected");
  }

  return shift @samples;
}

sub find_infinium_gex_sample {
  my ($self, $plate_barcode, $map) = @_;

  defined $plate_barcode or
    $self->logconfess("The plate_barcode argument was undefined");
  defined $map or $self->logconfess("The map argument was undefined");

  my ($barcode_prefix, $barcode) =
    $plate_barcode =~ /^([A-Z]{2})([0-9]+)[A-Z]$/;

  defined $barcode_prefix or
     $self->logconfess("Invalid plate barcode '$plate_barcode': ",
                       "failed to find the barcode prefix");
  defined $barcode or
    $self->logconfess("Invalid plate barcode '$plate_barcode': ",
                      "failed to find the barcode");

  my $query =
    qq(SELECT DISTINCT
          sm.internal_id,
          sm.sanger_sample_id,
          sm.consent_withdrawn,
          HEX(sm.uuid),
          sm.name,
          sm.common_name,
          sm.supplier_name,
          sm.accession_number,
          sm.gender,
          sm.cohort,
          sm.control,
          aq.study_internal_id AS study_id,
          pl.barcode_prefix,
          pl.barcode,
          pl.plate_purpose_name,
          wl.map
       FROM
         current_samples sm,
         current_wells wl,
         current_aliquots aq,
         current_plates pl,
         current_plate_purposes pp
       WHERE pl.barcode = ?
       AND pl.barcode_prefix = ?
       AND wl.map = ?
       AND aq.sample_internal_id = sm.internal_id
       AND wl.internal_id = aq.receptacle_internal_id
       AND pl.barcode = wl.plate_barcode
       AND pl.barcode_prefix = wl.plate_barcode_prefix
       AND pl.plate_purpose_internal_id = pp.internal_id
       AND pp.name like '%GEX%');

  my $sth = $self->dbh->prepare($query);

  $self->trace("Executing: '$query' with args [",
               "$barcode, $barcode_prefix, $map]");
  $sth->execute($barcode, $barcode_prefix, $map);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n records for sample '$plate_barcode' $map ",
                      "were returned where 1 was expected");
  }

  return shift @samples;
}

sub find_infinium_gex_sample_by_sanger_id {
  my ($self, $sanger_sample_id) = @_;

  defined $sanger_sample_id or
    $self->logconfess("The sanger_sample_id argument was undefined");

  my $query =
    qq(SELECT DISTINCT
          sm.internal_id,
          sm.sanger_sample_id,
          sm.consent_withdrawn,
          HEX(sm.uuid),
          sm.name,
          sm.common_name,
          sm.supplier_name,
          sm.accession_number,
          sm.gender,
          sm.cohort,
          sm.control,
          aq.study_internal_id AS study_id,
          pl.barcode_prefix,
          pl.barcode,
          pl.plate_purpose_name,
          wl.map
       FROM
         current_samples sm,
         current_wells wl,
         current_aliquots aq,
         current_plates pl,
         current_plate_purposes pp
       WHERE sm.sanger_sample_id = ?
       AND aq.sample_internal_id = sm.internal_id
       AND wl.internal_id = aq.receptacle_internal_id
       AND pl.barcode = wl.plate_barcode
       AND pl.barcode_prefix = wl.plate_barcode_prefix
       AND pl.plate_purpose_internal_id = pp.internal_id
       AND pp.name like '%GEX%');

  my $sth = $self->dbh->prepare($query);

  $self->trace("Executing: '$query' with arg [$sanger_sample_id]");
  $sth->execute($sanger_sample_id);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n records for sample '$sanger_sample_id' ",
                      "were returned where 1 was expected");
  }

  return shift @samples;
}

=head2 find_fluidigm_plate

  Arg [1]    : string
  Example    : $db->find_fluidigm_plate('Fluidigm plate barcode')
  Description: Return plate details for an Infinium LIMS plate barcode
               as a hashref with plate addresses as keys and values being
               a further hashref for each sample having the following keys
               and values:
               { internal_id,      => <SequenceScape plate id>,
                 sanger_sample_id  => <WTSI sample name string>,
                 consent_withdrawn => <boolean, true if now unconsented>,
                 uuid              => <SequenceScape UUID blob as hexidecimal>,
                 name              => <SequenceScape sample name>,
                 common_name       => <SequenceScape sample common name>,
                 supplier_name     => <Supplier provided name, may be undef>,
                 gender            => <Supplier gender string>,
                 cohort            => <Supplier cohort string>,
                 control           => <Supplier control flag>,
                 study_id          => <SequenceScape study id>,
                 barcode_prefix    => <SequenceScape barcode prefix string>,
                 barcode           => <SequenceScape barcode integer>,
                 map               => <SequenceScape well address string
                                       without 0-pad e.g A1> }
  Returntype : hashref
  Caller     : general

=cut

sub find_fluidigm_plate {
  my ($self, $fluidigm_barcode) = @_;

  defined $fluidigm_barcode or
    $self->logconfess("The fluidigm_barcode argument was undefined");

  my $query =
    qq(SELECT
         sm.internal_id,
         sm.sanger_sample_id,
         sm.consent_withdrawn,
         HEX(sm.uuid),
         sm.name,
         sm.common_name,
         sm.supplier_name,
         sm.accession_number,
         sm.gender,
         sm.cohort,
         sm.control,
         aq.study_internal_id AS study_id,
         pl.barcode_prefix,
         pl.barcode,
         pl.plate_purpose_name,
         wl.map
       FROM
         current_plates pl,
         current_samples sm,
         current_wells wl,
         current_aliquots aq
       WHERE
         pl.fluidigm_barcode = ?
         AND wl.plate_barcode = pl.barcode
         AND wl.plate_barcode_prefix = pl.barcode_prefix
         AND wl.internal_id = aq.receptacle_internal_id
         AND aq.sample_internal_id = sm.internal_id);

  $self->trace("Executing: '$query' with args [$fluidigm_barcode]");

  my $sth = $self->dbh->prepare($query);
  $sth->execute($fluidigm_barcode);

  my %plate;
  while (my $row = $sth->fetchrow_hashref) {
    $plate{$row->{map}} = $row;
  }

  return \%plate;
}

=head2 find_fluidigm_sample_by_plate

  Arg [1]    : string plate barcode
  Arg [2]    : string well map
  Example    : $db->find_sample_by_fluidigm_plate('Fluidigm plate barcode', )
  Description: Return plate details for an Infinium LIMS plate barcode
               as a hashref with plate addresses as keys and values being
               a further hashref for each sample having the following keys
               and values:
               { internal_id,      => <SequenceScape plate id>,
                 sanger_sample_id  => <WTSI sample name string>,
                 consent_withdrawn => <boolean, true if now unconsented>,
                 uuid              => <SequenceScape UUID blob as hexidecimal>,
                 name              => <SequenceScape sample name>,
                 common_name       => <SequenceScape sample common name>,
                 supplier_name     => <Supplier provided name, may be undef>,
                 gender            => <Supplier gender string>,
                 cohort            => <Supplier cohort string>,
                 control           => <Supplier control flag>,
                 study_id          => <SequenceScape study id>,
                 barcode_prefix    => <SequenceScape barcode prefix string>,
                 barcode           => <SequenceScape barcode integer>,
                 map               => <SequenceScape well address string
                                       without 0-pad e.g A1> }
  Returntype : hashref
  Caller     : general

=cut

sub find_fluidigm_sample_by_plate {
  my ($self, $fluidigm_barcode, $map) = @_;

  defined $fluidigm_barcode or
    $self->logconfess("The fluidigm_barcode argument was undefined");
  defined $map or $self->logconfess("The map argument was undefined");

  my $query =
    qq(SELECT
         sm.internal_id,
         sm.sanger_sample_id,
         sm.consent_withdrawn,
         HEX(sm.uuid),
         sm.name,
         sm.common_name,
         sm.supplier_name,
         sm.accession_number,
         sm.gender,
         sm.cohort,
         sm.control,
         aq.study_internal_id AS study_id,
         pl.barcode_prefix,
         pl.barcode,
         pl.plate_purpose_name,
         wl.map
       FROM
         current_plates pl,
         current_samples sm,
         current_wells wl,
         current_aliquots aq
       WHERE
         pl.fluidigm_barcode = ?
         AND wl.map = ?
         AND wl.plate_barcode = pl.barcode
         AND wl.plate_barcode_prefix = pl.barcode_prefix
         AND wl.internal_id = aq.receptacle_internal_id
         AND aq.sample_internal_id = sm.internal_id);

  $self->trace("Executing: '$query' with args [$fluidigm_barcode $map]");

  my $sth = $self->dbh->prepare($query);
  $sth->execute($fluidigm_barcode, $map);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n samples were returned where 1 sample was expected");
  }

  return shift @samples;
}

sub find_sample_studies {
  my ($self, $sample_id) = @_;

  $self->logwarn('Warehouse::find_sample_studies is deprecated');

  my $query =
    qq(SELECT DISTINCT
         st.internal_id,
         st.name,
         st.accession_number,
         st.study_title,
         st.study_type
      FROM
         current_samples sm,
         current_study_samples ss,
         current_studies st
       WHERE
         sm.internal_id = ?
         AND ss.sample_internal_id = sm.internal_id
         AND st.internal_id = ss.study_internal_id);

  $self->trace("Executing: '$query' with arg [$sample_id]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($sample_id);

  my @studies;
  while (my $row = $sth->fetchrow_hashref) {
    push(@studies, $row);
  }

  return \@studies;
}

sub find_study_title {
  my ($self, $study_id) = @_;

  $self->logwarn('Warehouse::find_study_title is deprecated');

  my $query = qq(SELECT study_title
                 FROM current_studies
                 WHERE internal_id = ?);
  my $sth = $self->dbh->prepare($query);
  $sth->execute($study_id);

  my @titles;
  while (my $row = $sth->fetchrow_hashref) {
    push(@titles, $row->{study_title});
  }

  my $n = scalar @titles;
  if ($n > 1) {
    $self->logconfess("$n records for study '$study_id' were ",
                      "returned where 1 was expected");
  }

  return shift @titles;
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Database::Warehouse

=head1 DESCRIPTION

A class for querying the SequenceScape warehouse database to retrieve
details of samples for genotyping analysis.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
