
use utf8;

package WTSI::NPG::Genotyping::Database::SNP;

use Cache::Cache qw($EXPIRES_NEVER);
use Moose;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;

extends 'WTSI::NPG::Database';

with 'WTSI::NPG::Database::DBI', 'WTSI::DNAP::Utilities::Cacheable';

# Method names for MOP operations
our $FIND_SEQUENOM_PLATE_ID  = 'find_sequenom_plate_id';
our $FIND_PLATE_STATUS       = 'find_plate_status';
our $FIND_PLATE_WELLS_STATUS = '_find_plate_wells_status';

our $PLATE_STATUS_GENOTYPING_DONE   = 'Genotyping Done';
our $PLATE_STATUS_GENOTYPING_FAILED = 'Genotyping Failed';
our $WELL_STATUS_GENOTYPING_DONE    = 'OK';
our $WELL_STATUS_GENOTYPING_FAILED  = 'No call';

my $meta = __PACKAGE__->meta;

around $FIND_SEQUENOM_PLATE_ID => sub {
  my ($orig, $self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  my $cache = $self->get_method_cache
    ($meta->get_method($FIND_SEQUENOM_PLATE_ID), {default_expires_in => 600});
  my $key = $plate_name;

  return $self->get_with_cache($cache, $key, $orig, $plate_name);
};

=head2 find_sequenom_plate_id

  Arg [1]    : A plate name from the Sequenom LIMS

  Example    : $db->find_sequenom_plate_id('my_plate_name')
  Description: Given the name of a Sequenom LIMS plate, return the
               Sequencescape identifier ("internal_id") of that
               plate.

  Returntype : Str

=cut

sub find_sequenom_plate_id {
  my ($self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  my $query =
    qq(SELECT DISTINCT
         dpn.name AS plate_id
       FROM
         dna_plate          dp,
         dnaplate_status    dps,
         dnaplatestatusdict dpsd,
         dptypedict         dptd,
         dnaplate_name      dpn,
         dpnamedict         dpnd
       WHERE
         dp.plate_name = ?
       AND dp.id_dnaplate   = dps.id_dnaplate
       AND dps.status       = dpsd.id_dict
       AND dpsd.description = 'Imported to MSPEC1'
       AND dpn.id_dnaplate  = dp.id_dnaplate
       AND dpnd.id_dict     = dpn.name_type
       AND dpnd.description = 'SequenceScape_ID'
       AND dp.plate_type    = dptd.id_dict
       AND dptd.description = 'mspec');

  $self->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_name);

  my @plate_ids;
  while (my $row = $sth->fetchrow_array) {
    push @plate_ids, $row;
  }

  my $n = scalar @plate_ids;
  if ($n > 1) {
    $self->logconfess("$n plate identifiers were returned ",
                      "where 1 was expected: [", join(', ', @plate_ids), "]");
  }

  return shift @plate_ids;
}

=head2 find_sequenom_calls

  Arg [1]    : Array of sample names (individual.clonename in SNP)

  Example    : $db->find_sequenom_calls('ABC123', 'XYZ123')
  Description: Finds Sequenom results from the SNP. The 'name' is expected
               to match the individual.clonename in SNP.

               The linking value between the databases is the identifier
               described as:

                the 'sanger_sample_id' (in SequenceScape)

                the 'individual.clonename' (in SNP)

               As the genotyping pipeline database accepts both WTSI and
               externally-sourced samples from multiple centres, it uses a
               more generic term 'sample.name'. However, in the case of WTSI
               samples, this corresponds to the identifier described above.

               In cases where the same individual.clonename has been assayed
               multiple times with the same plex, the most recent result is
               taken.

  Returntype : HashRef

=cut

sub find_sequenom_calls {
  my ($self, $snpset, $sample_names) = @_;

  defined $sample_names or
    $self->logconfess('A defined sample_names argument is required');
  ref $sample_names eq 'ARRAY' or
    $self->logconfess('The sample_names argument must be an ArrayRef');

  my $query =
    qq(SELECT DISTINCT
         well_assay.id_well       AS assay_name,
         snp_summary.default_name AS snp_name,
         genotype.genotype        AS genotype,
         genotype.id_session      AS id_session,
         well_result.call_date    AS call_date
       FROM
         individual,
         genotype,
         well_result,
         well_assay,
         snpassay_snp,
         snp_summary
       WHERE
         individual.clonename      = ?
         AND well_assay.id_well    = ?
         AND genotype.id_ind       = individual.id_ind
         AND genotype.id_result    = well_result.id_result
         AND genotype.id_assay     = well_assay.id_assay
         AND well_assay.id_assay   = snpassay_snp.id_assay
         AND snpassay_snp.id_snp   = snp_summary.id_snp
         AND genotype.disregard    = 0
         AND well_result.call_date =
             (SELECT
                MAX(well_result.call_date)
              FROM
                individual, genotype, well_result, well_assay
              WHERE individual.clonename = ?
              AND well_assay.id_well     = ?
              AND genotype.id_ind        = individual.id_ind
              AND genotype.id_result     = well_result.id_result
              AND genotype.id_assay      = well_assay.id_assay
              AND genotype.disregard     = 0));

  my $sth = $self->dbh->prepare($query);

  my %result;

  foreach my $sample_name (@$sample_names) {
    $self->trace("Executing: '$query' with args [",
                 join(", ", $sample_name, $snpset->name,
                      $sample_name, $snpset->name),
                 "]");
    $sth->execute($sample_name, $snpset->name,
                  $sample_name, $snpset->name);

    my @calls;
    while (my ($assay_name, $snp_name, $genotype, $session, $call_date) =
           $sth->fetchrow_array) {
      # Genotypes are stored as single characters when both alleles
      # are the same. Convert to a pair of characters.
      $genotype .= $genotype if length($genotype) == 1;

      $self->debug("Got Sequenom call for sample '$sample_name' in SNP set '",
                   $snpset->name, "': '$genotype' for SNP '$snp_name'");

      my $snp = $snpset->named_snp($snp_name);

      if ($snp) {
        push @calls, WTSI::NPG::Genotyping::Call->new(genotype => $genotype,
                                                      snp      => $snp);
      }
      else {
        $self->debug("Ignoring Sequenom call on SNP '$snp_name' for sample ",
                     "'$sample_name' because SNP is not a member ",
                     "of SNP set '", $snpset->name, "'");
      }
    }

    $result{$sample_name} = \@calls;
  }

  return \%result;
}

=head2 find_updated_plate_names

  Arg [1]    : start DateTime
  Arg [2]    : end DateTime. Optional, defaults to start

  Example    : $db->find_updated_plate_names($then, $now)
  Description: Returns a list of names of plates whose status has been set
               to its current value between start and end dates, inclusive.
               Only the date part of the DateTime arguments is significant;
               these arguments are cloned and truncated prior to
               comparison.
  Returntype : ArrayRef[Str]

=cut

sub find_updated_plate_names {
  my ($self, $start_date, $end_date) = @_;

  defined $start_date or
    $self->logconfess('The start_date argument was undefined');

  $end_date ||= $start_date;

  my $start = $start_date->clone->truncate(to => 'day');
  my $end   = $end_date->clone->truncate(to => 'day');

  # Note the DISTINCT; it appears that the plate can be entered
  # multiple times with the same and can have multiple "current"
  # statuses (which should all be the same).
  my $query =
    qq(SELECT DISTINCT
         dp.plate_name
       FROM
         dna_plate          dp,
         dnaplate_status    dps,
         dnaplatestatusdict dpsd,
         dptypedict         dptd
       WHERE
           trunc(dps.status_date) >= to_date(?, 'YYYY-MM-DD')
       AND trunc(dps.status_date) <= to_date(?, 'YYYY-MM-DD')
       AND dp.id_dnaplate   = dps.id_dnaplate
       AND dp.plate_type    = dptd.id_dict
       AND dptd.description = 'mspec'
       AND dps.is_current   = 1
       AND dps.status       = dpsd.id_dict);

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

=head2 find_plate_passed

  Arg [1]    : A plate name from the Sequenom LIMS

  Example    : $db->find_plate_passed('my_plate_name')
  Description: Given the name of a Sequenom LIMS plate, return true if the
               plate has passed genotyping QC.

  Returntype : Bool

=cut

sub find_plate_passed {
  my ($self, $plate_name) = @_;

  my $status = $self->find_plate_status($plate_name);

  defined $status or
    $self->logconfess("No plate status was found for plate '$plate_name'");

  return $status eq $PLATE_STATUS_GENOTYPING_DONE;
}

=head2 find_plate_failed

  Arg [1]    : A plate name from the Sequenom LIMS

  Example    : $db->find_plate_failed('my_plate_name')
  Description: Given the name of a Sequenom LIMS plate, return true if the
               plate has failed genotyping QC.

  Returntype : Bool

=cut

sub find_plate_failed {
  my ($self, $plate_name) = @_;

  my $status = $self->find_plate_status($plate_name);

  defined $status or
    $self->logconfess("No plate status was found for plate '$plate_name'");

  return $status eq $PLATE_STATUS_GENOTYPING_FAILED;
}

around $FIND_PLATE_STATUS => sub {
  my ($orig, $self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  my $cache = $self->get_method_cache($meta->get_method($FIND_PLATE_STATUS),
                                      {default_expires_in => 600});
  my $key = $plate_name;

  return $self->get_with_cache($cache, $key, $orig, $plate_name);
};

=head2 find_plate_status

  Arg [1]    : A plate name from the Sequenom LIMS

  Example    : $db->find_plate_status('my_plate_name')
  Description: Given the name of a Sequenom LIMS plate, return the QC status
               string used by SNP to denote pass/fail.

  Returntype : Str

=cut

sub find_plate_status {
  my ($self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  my $query =
    qq(SELECT description FROM
         (SELECT
            dpsd.description, dps.status_date
          FROM
            dna_plate          dp,
            dnaplate_status    dps,
            dnaplatestatusdict dpsd,
            dptypedict         dptd
          WHERE
            dp.plate_name = ?
          AND dp.id_dnaplate   = dps.id_dnaplate
          AND dp.plate_type    = dptd.id_dict
          AND dptd.description = 'mspec'
          AND dps.is_current   = 1
          AND dps.status       = dpsd.id_dict
          ORDER BY dps.status_date DESC)
        WHERE ROWNUM <= 1);

  $self->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_name);

  my @status;
  while (my $row = $sth->fetchrow_array) {
    push @status, $row;
  }

  my $n = scalar @status;
  if ($n > 1) {
    $self->logconfess("$n plate statues were returned ",
                      "where 1 was expected: [", join(', ', @status), "]");
  }

  return shift @status;
}

=head2 find_well_passed

  Arg [1]    : A plate name from the Sequenom LIMS
  Arg [2]    : A padded well address e.g. A01

  Example    : $db->find_well_passed('my_plate_name', 'A01')
  Description: Given the name of a Sequenom LIMS plate, return true if the
               well has passed genotyping QC.

  Returntype : Bool

=cut

sub find_well_passed {
  my ($self, $plate_name, $map) = @_;

  my $status = $self->find_well_status($plate_name, $map);

  defined $status or
    $self->logconfess("No well status was found for well '$plate_name : $map'");

  return $status eq $WELL_STATUS_GENOTYPING_DONE;
}

=head2 find_well_failed

  Arg [1]    : A plate name from the Sequenom LIMS
  Arg [2]    : A padded well address e.g. A01

  Example    : $db->find_well_failed('my_plate_name', 'A01')
  Description: Given the name of a Sequenom LIMS plate, return true if the
               well has failed genotyping QC.

  Returntype : Bool

=cut

sub find_well_failed {
  my ($self, $plate_name, $map) = @_;

  my $status = $self->find_well_status($plate_name, $map);

  defined $status or
    $self->logconfess("No well status was found for well '$plate_name : $map'");

  return $status eq $WELL_STATUS_GENOTYPING_FAILED;
}

=head2 find_well_status

  Arg [1]    : A plate name from the Sequenom LIMS
  Arg [2]    : A padded well address e.g. A01

  Example    : $db->find_well_status('my_plate_name', 'A01')
  Description: Given the name of a Sequenom LIMS well, return the QC status
               string used by SNP to denote pass/fail. This is 'OK' for a
               pass and 'No call' for a fail.

  Returntype : Str

=cut

sub find_well_status {
  my ($self, $plate_name, $map) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  defined $map or $self->logconfess('A defined map argument is required');
  $map or $self->logconfess('A non-empty map argument is required');

  if ($self->find_plate_failed($plate_name)) {
    return $WELL_STATUS_GENOTYPING_FAILED;
  }
  elsif ($self->find_plate_passed($plate_name)) {
    return $self->_find_plate_wells_status($plate_name)->{$map};
  }
}

# Reduce database access to one hit per plate
around $FIND_PLATE_WELLS_STATUS => sub {
  my ($orig, $self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  my $cache = $self->get_method_cache
    ($meta->get_method($FIND_PLATE_WELLS_STATUS), {default_expires_in => 180});
  my $key = $plate_name;

  return $self->get_with_cache($cache, $key, $orig, $plate_name);
};

sub _find_plate_wells_status {
  my ($self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('A defined plate_name argument is required');
  $plate_name or
    $self->logconfess('A non-empty plate_name argument is required');

  my $query =
    qq(SELECT DISTINCT
         CONCAT(UPPER(mp.maprow), LPAD(mp.mapcol, 2, '0')) AS well,
         gdrd.description                                  AS status
       FROM
         dna_plate          dp,
         dnaplate_status    dps,
         dnaplatestatusdict dpsd,
         dptypedict         dptd,
         dna_well           wl,
         rowcol_map         mp,
         well_assay,
         genotype,
         gtdisregarddict   gdrd
       WHERE
        dp.plate_name = ?
       AND dp.id_dnaplate        = dps.id_dnaplate
       AND dp.plate_type         = dptd.id_dict
       AND dptd.description      = 'mspec'
       AND dps.is_current        = 1
       AND dps.status            = dpsd.id_dict
       AND dpsd.description      = ?
       AND wl.id_dnaplate        = dp.id_dnaplate
       AND genotype.id_dnawell   = wl.id_dnawell
       AND genotype.id_assay     = well_assay.id_assay
       AND genotype.disregard    = gdrd.id_dict
       AND mp.id_map             = wl.id_map
       ORDER BY well);

  my $desc = $PLATE_STATUS_GENOTYPING_DONE;
  $self->trace("Executing: '$query' with args [$plate_name, $desc]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_name, $desc);

  my %plate;
  my $well_count = 0;
  while (my $well = $sth->fetchrow_hashref) {
    my $position = $well->{WELL};
    $plate{$position} = $well->{STATUS};
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

WTSI::NPG::Genotyping::Database::SNP

=head1 DESCRIPTION

A class for querying the WTSI SNP database to retrieve the result of
Sequenom analyses.

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
