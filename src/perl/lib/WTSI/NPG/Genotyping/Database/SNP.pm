
use utf8;

package WTSI::NPG::Genotyping::Database::SNP;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNP;
use WTSI::NPG::Genotyping::SNPSet;

use Moose;

extends 'WTSI::NPG::Database';

=head2 find_sequenom_plate_id

  Arg [1]    : A plate name from the Sequenom LIMS

  Example    : $db->find_sequenom_plate_id('my_plate_name')
  Description: Given the name of a Sequenom LIMS plate, return the
               Sequencescape identifier ("internal_id") of that
               plate.

  Returntype : string

=cut

sub find_sequenom_plate_id {
  my ($self, $plate_name) = @_;

  defined $plate_name or
    $self->logconfess('The plate_name argument was undefined');

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

  $self->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($plate_name);

  my @plate_ids;
  while (my $row = $sth->fetchrow_array) {
    push(@plate_ids, $row);
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
         well_assay.id_well AS assay_name,
         snp_name.snp_name AS snp_name,
         genotype.genotype AS genotype
       FROM
         well_assay,
         snpassay_snp,
         snp_name,
         mapped_snp,
         genotype,
         individual
       WHERE
         well_assay.id_assay = snpassay_snp.id_assay
         AND snpassay_snp.id_snp = snp_name.id_snp
         AND mapped_snp.id_snp = snp_name.id_snp
         AND (snp_name.snp_name_type = 1 OR snp_name.snp_name_type = 6)
         AND genotype.id_assay = snpassay_snp.id_assay
         AND genotype.id_ind = individual.id_ind
         AND disregard = 0
         AND confidence <> 'A'
         AND individual.clonename = ?);

  my $sth = $self->dbh->prepare($query);

  my %result;

  foreach my $sample_name (@$sample_names) {
    $self->trace("Executing: '$query' with args [$sample_name]");
    $sth->execute($sample_name);

    my @calls;
    while (my ($assay_name, $snp_name, $genotype) = $sth->fetchrow_array) {
      # Selecting WHERE well_assay.id_well = $assay_name in the query
      # causes a performance problem; optimiser tries lots of nested
      # loop joins
      unless ($assay_name eq $snpset->name) {
        $self->debug("Ignoring Sequenom result for sample '$sample_name' ",
                     "SNP '$snp_name' because assay '$assay_name' is not ",
                     "SNP set '", $snpset->name, "'");
      }

      # Genotypes are stored as single characters when both alleles
      # are the same. Convert to a pair of characters.
      $genotype .= $genotype if length($genotype) == 1;
      # SNP name sometimes has junk prefix:
      $snp_name =~ s/^\S+;(\S+)$/$1/;

      $self->debug("Got Sequenom call for sample '$sample_name' in SNP set '",
                   $snpset->name, "': '$genotype' for SNP '$snp_name'");

      # May get >1 item for a name for gender markers. Just use the first.
      my @named = $snpset->named_snp($snp_name);
      my $snp = shift @named;

      if ($snp) {
        push @calls, WTSI::NPG::Genotyping::Call->new(genotype => $genotype,
                                                      snp      => $snp);
      }
      else {
        $self->warn("Ignoring Sequenom call on SNP '$snp_name' for sample ",
                    "'$sample_name' because SNP is not a member of SNP set '",
                    $snpset->name, "'");
      }
    }

    $result{$sample_name} = \@calls;
  }

  return \%result;
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

=cut

sub find_sequenom_calls_by_sample {
  my $self = shift;

  my %sample_ids = %{ shift() };
  my (%sqnm_calls, %sqnm_snps, %missing_samples);

  my $sth = $self->dbh->prepare(qq(
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

  return (\%sqnm_calls, \%sqnm_snps, \%missing_samples, $total_calls);
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
