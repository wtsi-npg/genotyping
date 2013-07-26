use utf8;

package WTSI::NPG::Genotyping::Database::SNP;

use strict;
use warnings;

use base 'WTSI::NPG::Database';

=head2 find_sequenom_plate_id

  Arg [1]    : A plate name from the Sequenom LIMS
  Example    : $db->find_sequenom_plate_id('my_plate_name')
  Description: Given the name of a Sequenom LIMS plate, return the
               Sequencescape identifier ("internal_id") of that
               plate.

  Returntype : string
  Caller     : general

=cut

sub find_sequenom_plate_id {
  my ($self, $plate_name) = @_;

  my $dbh = $self->dbh;

  my $query =
    qq(SELECT
         dpn.name AS plate_id
       FROM
         dna_plate dp,
         dnaplate_status dps,
         dnaplatestatusdict dpsd,
         dptypedict dptd,
         dnaplate_name dpn,
         dpnamedict dpnd
       WHERE
         dp.plate_name = ?
       AND dp.id_dnaplate = dps.id_dnaplate
       AND dps.status = dpsd.id_dict
       AND dpsd.description = 'Imported to MSPEC1'
       AND dpn.id_dnaplate = dp.id_dnaplate
       AND dpnd.id_dict = dpn.name_type
       AND dpnd.description = 'SequenceScape_ID'
       AND dp.plate_type = dptd.id_dict
       AND dptd.description = 'mspec'
       ORDER BY dps.status_date DESC);

  $self->log->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $dbh->prepare($query);
  $sth->execute($plate_name);

  my @plate_ids;
  while (my $row = $sth->fetchrow_array) {
    push(@plate_ids, $row);
  }

  my $n = scalar @plate_ids;
  if ($n > 1) {
    $self->log->logconfess("$n plate identifiers were returned ",
                           "where 1 was expected: [",
                           join(', ', @plate_ids), "]");
  }

  return shift @plate_ids;
}

=head2 insert_sequenom_calls

  Arg [1]    : WTSI::NPG::Genotyping::Database::Pipeline object
  Arg [2]    : arrayref of WTSI::NPG::Genotyping::Schema::Result::Sample objects
  Example    : $db->insert_sequenom_calls($pipedb, $samples)
  Description: Inserts Sequenom results from the SNP database into the
               pipeline database. The 'name' field of the Sample is expected
               to match the individual.clonename in SNP.

               The linking value between the databases is the identifier
               described as:

                the 'sanger_sample_id' (in SequenceScape)
                the 'individual.clonename' (in SNP)

               As the genotyping pipeline database accepts both WTSI and
               externally-sourced samples from multiple centres, it uses a
               more generic term 'sample.name'. However, in the case of WTSI
               samples, this corresponds to the identifier described above.

  Returntype : integer (total number of
               WTSI::NPG::Genotyping::Schema::Result::SnpResults inserted)
  Caller     : general

=cut

sub insert_sequenom_calls {
  my ($self, $pipedb, $samples) = @_;

  my $dbh = $self->dbh;

  my $query =
    qq(SELECT DISTINCT
         snp_name.snp_name,
         snp_sequence.chromosome,
         mapped_snp.position,
         genotype.genotype
       FROM
         well_assay,
         snpassay_snp,
         snp_name,
         mapped_snp,
         snp_sequence,
         genotype,
         individual
       WHERE
         well_assay.id_assay = snpassay_snp.id_assay
         AND snpassay_snp.id_snp = snp_name.id_snp
         AND mapped_snp.id_snp = snp_name.id_snp
         AND snp_sequence.id_sequence = mapped_snp.id_sequence
         AND (snp_name.snp_name_type = 1 OR snp_name.snp_name_type = 6)
         AND genotype.id_assay = snpassay_snp.id_assay
         AND genotype.id_ind = individual.id_ind
         AND disregard = 0
         AND confidence <> 'A'
         AND individual.clonename = ?);

  my $sth = $dbh->prepare($query);

  my $snpset = $pipedb->snpset->find({name => 'Sequenom'});
  my $method = $pipedb->method->find({name => 'Sequenom'});

  my $count = 0;
  foreach my $sample (@$samples) {

    if ($sample->include && defined $sample->sanger_sample_id) {
      my $id = $sample->sanger_sample_id;

      $self->log->trace("Executing: '$query' with args [$id]");
      $sth->execute($id);

      my $result = $sample->add_to_results({method => $method});

      while (my ($name, $chromosome, $position, $genotype) =
             $sth->fetchrow_array) {
        $genotype .= $genotype if length($genotype) == 1;

        my $snp = $pipedb->snp->find_or_create
          ({name => $name,
            chromosome => $chromosome,
            position => $position,
            snpset => $snpset});

        $result->add_to_snp_results({snp => $snp,
                                     value => $genotype});
        ++$count;
      }
    }
  }

  return $count;
}

=head2 find_sequenom_calls_by_sample

  Arg [1]    : WTSI::NPG::Genotyping::Database::Pipeline object
  Arg [2]    : Reference to a hash. Keys are sample names, values are
               sample IDs in the SNP database.
  Description: Query the SNP database by sample and return details of calls.
               Returns a hash of hashes of calls indexed by sample and SNP; 
               hash of SNPs found; hash of samples with no calls in SNP;
               total number of calls read
  Returntype : (hashref, hashref, hashref, integer)
  Caller     : General

=cut

sub find_sequenom_calls_by_sample {
    my $self = shift;
    my %sample_ids = %{ shift() };
    my ($dbh, $sth, %sqnm_calls, %sqnm_snps, %missing_samples);
    $dbh = $self->dbh;
    $sth = $dbh->prepare(qq(
  SELECT DISTINCT
    well_assay.id_well,
    snp_name.snp_name,
    genotype.genotype,
    genotype.confidence,
    genotype.disregard
  FROM
    well_assay,
    snpassay_snp,
    snp_name,
    genotype,
    individual
  WHERE
    well_assay.id_assay = snpassay_snp.id_assay
  AND snpassay_snp.id_snp = snp_name.id_snp
  AND (snp_name.snp_name_type = 1 OR snp_name.snp_name_type = 6)
  AND genotype.id_assay = snpassay_snp.id_assay
  AND genotype.id_ind = individual.id_ind
  AND disregard = 0
  AND confidence <> 'A'
  AND individual.clonename = ?));

    my $total_calls = 0;
    foreach my $sample (keys(%sample_ids)) {
      $sth->execute($sample_ids{$sample}); # query DB with sample ID

      foreach my $row (@{$sth->fetchall_arrayref}) {
        my ($well, $snp, $call, $conf, $disregard) = @{$row};
        # $disregard==0 by construction of DB query
        $call .= $call if length($call) == 1;   # sqnm may have "A" ~ "AA"
        next if $call =~ /[N]{2}/;              # skip 'NN' calls

        $sqnm_calls{$sample}{$snp} = $call;
        $sqnm_snps{$snp} = 1;
        $total_calls += 1;
      }

      if (!$sqnm_calls{$sample}) {
        $missing_samples{$sample} = 1;
      }
    }
    $sth->finish;
    $dbh->disconnect;
    return (\%sqnm_calls, \%sqnm_snps, \%missing_samples, $total_calls);
}

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
