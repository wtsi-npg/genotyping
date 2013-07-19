use utf8;

package WTSI::NPG::Genotyping::Database::Sequenom;

use strict;
use warnings;
use Carp;

use base 'WTSI::NPG::Database';


=head2 find_finished_plate_names

  Arg [1]    : start DateTime
  Arg [2]    : end DateTime. Optional, defaults to start
  Example    : $db->find_finished_plate_names($then, $now)
  Description: Returns a list of names of plates finished between start and end
               dates, inclusive. Only the date part of the DateTime arguments is
               significant; these arguments are cloned and truncated prior to
               comparison.
  Returntype : arrayref of string
  Caller     : general

=cut

sub find_finished_plate_names {
  my ($self, $start_date, $end_date) = @_;
  unless (defined $start_date) {
    confess "The start_date argument was not defined\n";
  }

  $end_date ||= $start_date;

  my $start = $start_date->clone->truncate(to => 'day');
  my $end = $end_date->clone->truncate(to => 'day');

  my $dbh = $self->dbh;

  my $query = qq(SELECT
                   plate_id
                 FROM
                   plate
                 WHERE
                   status = 'finished'
                 AND trunc(date_time_stamp) >= to_date(?, 'YYYY-MM-DD')
                 AND trunc(date_time_stamp) <= to_date(?, 'YYYY-MM-DD'));

  if ($start->compare($end) > 0) {
    $self->log->logconfess("Start date '$start' was after end date '$end'");
  }

  $self->log->trace("Executing: '$query' with args [$start, $end]");
  my $sth = $dbh->prepare($query);
  $sth->execute($start->ymd('-'), $end->ymd('-'));

  my @plates;
  while (my $row = $sth->fetchrow_arrayref) {
    push(@plates, $row->[0]);
  }

  return \@plates;
}

=head2 find_plate_results

  Arg [1]    : string
  Example    : $db->find_plate_results('plate name')
  Description: Returns details of the Sequenom assay results for a plate.
               The plate's results are returned as a hashref keyed on well
               address. Each hash value is an arrayref of result records
               which are themselves hashrefs with the folowwing keys and
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
  Caller     : general

=cut

sub find_plate_results {
  my ($self, $plate_name) = @_;
  unless (defined $plate_name) {
    confess "The plate_name argument was not defined\n";
  }

  my $dbh = $self->dbh;

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

  $self->log->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $dbh->prepare($query);
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
    $self->log->logconfess("No wells were found for plate '$plate_name'");
  }
  unless ($well_count % 96 == 0) {
    $self->log->warn("Found $well_count wells for plate '$plate_name'");
  }

  return \%plate;
}

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
