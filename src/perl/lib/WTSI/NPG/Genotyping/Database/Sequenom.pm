
use utf8;

package WTSI::NPG::Genotyping::Database::Sequenom;

use Carp;
use Moose;

extends 'WTSI::NPG::Database';

with 'WTSI::NPG::Database::DBI';

=head2 find_finished_plate_names

  Arg [1]    : start DateTime
  Arg [2]    : end DateTime. Optional, defaults to start

  Example    : my @names = @{$db->find_finished_plate_names($then, $now)}
  Description: Returns a list of names of plates finished between start and end
               dates, inclusive. Only the date part of the DateTime arguments is
               significant; these arguments are cloned and truncated prior to
               comparison.

               NB: The LIMS does not update the timestamp when the plate status
               changes to 'finished'. This means that the results of this method
               will always be approximate.
  Returntype : ArrayRef[Str]

=cut

sub find_finished_plate_names {
  my ($self, $start_date, $end_date) = @_;

  defined $start_date or
    $self->logconfess('The start_date argument was undefined');

  $end_date ||= $start_date;

  my $start = $start_date->clone->truncate(to => 'day');
  my $end = $end_date->clone->truncate(to => 'day');

  my $query = qq(SELECT
                   plate_id
                 FROM
                   plate
                 WHERE
                   status = 'finished'
                 AND trunc(date_time_stamp) >= to_date(?, 'YYYY-MM-DD')
                 AND trunc(date_time_stamp) <= to_date(?, 'YYYY-MM-DD'));

  if ($start->compare($end) > 0) {
    $self->logconfess("Start date '$start' was after end date '$end'");
  }

  $self->trace("Executing: '$query' with args [$start, $end]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($start->ymd('-'), $end->ymd('-'));

  my @plates;
  while (my $row = $sth->fetchrow_arrayref) {
    push(@plates, $row->[0]);
  }

  return \@plates;
}

=head2 find_plate_result_wells

  Arg [1]    : string

  Example    : my @wells = @{$db->find_plate_result_wells('plate name')}
  Description: Returns a list of all the well names that have result data
               associated with them.

  Returntype : ArrayRef[Str]

=cut

sub find_plate_result_wells {
  my ($self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('The plate_name argument was undefined');

  my $query =
    qq(SELECT DISTINCT
         al.well AS "well"
       FROM SEQUENOM.PLATE pl, SEQUENOM.SR_ALLELOTYPE_2 al
       WHERE
         pl.plate_id = ?
       AND pl.plate_id = al.plate
       AND pl.status = 'finished'
       AND al.oligo_type <> 'P'
       AND al.allele <> 'Pausing Peak'
       ORDER BY al.well);

  $self->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_name);

  my @wells;
  while (my $well = $sth->fetchrow_hashref) {
    push(@wells, $well->{well});
  }

  return \@wells;
}

=head2 find_plate_results

  Arg [1]    : string

  Example    : my @results = @{$db->find_plate_results('plate name')}
  Description: Returns details of the Sequenom assay results for a plate.
               The plate's results are returned as a hashref keyed on well
               address. Each hash value is an arrayref of result records
               which are themselves hashrefs with the following keys and
               values:
               {customer   => <customer name string>,
                project    => <project name string>,
                experiment => <experiment name string>,
                chip       => <chip number>,
                well       => <well address string with 0-pad e.g A01>,
                assay      => <assay identifier string>,
                genotype   => <genotype string>,
                sample     => <sample string>,
                allele     => <allele string>,
                mass       => <assay result peak mass numeric value>,
                height     => <assay result peak height numeric value>}

  Returntype : hashref of arrayrefs of hashrefs

=cut

sub find_plate_results {
  my ($self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('The plate_name argument was undefined');

  my $query =
    qq(SELECT
         al.customer   AS "customer",
         al.project    AS "project",
         al.plate      AS "plate",
         al.experiment AS "experiment",
         al.chip       AS "chip",
         al.well       AS "well",
         al.assay      AS "assay",
         al.genotype   AS "genotype",
         al.status     AS "status",
         al.sample     AS "sample",
         al.allele     AS "allele",
         al.mass       AS "mass",
         al.height     AS "height"
       FROM SEQUENOM.PLATE pl, SEQUENOM.SR_ALLELOTYPE_2 al
       WHERE
         pl.plate_id = ?
       AND pl.plate_id = al.plate
       AND pl.status = 'finished'
       AND al.oligo_type <> 'P'
       AND al.allele <> 'Pausing Peak'
       ORDER BY al.well, al.assay, al.mass);

  $self->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_name);

  my %plate;
  my $well_count = 0;
  while (my $well = $sth->fetchrow_hashref) {
    my $position = $well->{well};
    unless (exists $plate{$position}) {
      $plate{$position} = [];
    }

    push(@{$plate{$position}}, $well);
    ++$well_count;
  }

  unless ($well_count > 0) {
    $self->logconfess("No wells were found for plate '$plate_name'");
  }
  unless ($well_count % 96 == 0) {
    $self->warn("Found $well_count wells for plate '$plate_name'");
  }

  return \%plate;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Database::Sequenom

=head1 DESCRIPTION

A class for querying the WTSI SNP database to retrieve the result of
Sequenom analyses.

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
