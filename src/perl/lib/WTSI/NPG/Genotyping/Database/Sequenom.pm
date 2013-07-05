use utf8;

package WTSI::NPG::Genotyping::Database::Sequenom;

use strict;
use warnings;

use base 'WTSI::NPG::Database';

sub find_assay_records {
  my ($self, $plate_name) = @_;

  my $dbh = $self->dbh;

  my $query =
    qq(SELECT
         customer,
         project,
         plate,
         experiment,
         chip,
         well AS WELL_POSITION,
         assay AS ASSAY_ID,
         genotype AS GENOTYPE_ID,
         status AS DESCRIPTION,
         sample AS SAMPLE_ID,
         allele,
         mass,
         height
       FROM SEQUENOM.SR_ALLELOTYPE_2
       WHERE
         plate = ?
       AND oligo_type <> 'P'
       AND allele <> 'Pausing Peak'
       ORDER BY well, assay, mass);

  $self->log->trace("Executing: '$query' with args [$plate_name]");
  my $sth = $dbh->prepare($query);
  $sth->execute($plate_name);

  my %wells;
  my $well_count = 0;
  while (my $row = $sth->fetchrow_hashref) {
    my $position = $row->{WELL_POSITION};
    unless (exists $wells{$position}) {
      $wells{$position} = [];
    }

    push(@{$wells{$position}}, $row);
    ++$well_count;
  }

  unless ($well_count > 0) {
    $self->log->logconfess("No wells were found for plate '$plate_name'");
  }
  unless ($well_count % 96 == 0) {
    $self->log->warn("Found $well_count wells for plate '$plate_name'");
  }

  return \%wells;
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
