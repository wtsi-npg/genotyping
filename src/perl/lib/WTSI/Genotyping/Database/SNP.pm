use utf8;

package WTSI::Genotyping::Database::SNP;

use strict;
use warnings;

use WTSI::Genotyping::Database;

our @ISA = qw(WTSI::Genotyping::Database);


=head2 insert_sequenom_calls

  Arg [1]    : WTSI::Genotyping::Database::Pipeline object
  Arg [2]    : arrayref of WTSI::Genotyping::Schema::Result::Sample objects
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
               WTSI::Genotyping::Schema::Result::SnpResults inserted)
  Caller     : general

=cut

sub insert_sequenom_calls {
  my ($self, $pipedb, $samples) = @_;

  my $dbh = $self->dbh;

  my $query =
    "SELECT DISTINCT
       snp_name.snp_name,
       snp_sequence.chromosome,
       mapped_snp.position,
       genotype.genotype
     FROM
       well_assay, snpassay_snp, snp_name, mapped_snp, snp_sequence, genotype, individual
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
       AND individual.clonename = ?";

  my $sth = $dbh->prepare($query);

  my $snpset = $pipedb->snpset->find({name => 'Sequenom'});
  my $method = $pipedb->method->find({name => 'Sequenom'});

  my $count = 0;
  foreach my $sample (@$samples) {
    if ($sample->include && defined $sample->sanger_sample_id) {
      $sth->execute($sample->sanger_sample_id);

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

1;

__END__

=head1 NAME

WTSI::Genotyping::Database::SNP

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
