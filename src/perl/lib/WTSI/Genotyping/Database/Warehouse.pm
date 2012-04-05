
package WTSI::Genotyping::Database::Warehouse;

use strict;
use warnings;
use Carp;

use WTSI::Genotyping::Database;

our @ISA = qw(WTSI::Genotyping::Database);


=head2 find_infinium_plate

  Arg [1]    : string
  Example    : $db->find_infinium_plate('Infinium LIMS plate barcode')
  Description: Returns plate details for an Infinium LIMS plate barcode
               as a hashref with the following keys and values:
               { sanger_sample_id => <WTSI sample name string>,
                 uuid             => <SequenceScape UUID string>,
                 gender           => <Supplier gender string>,
                 barcode_prefix   => <SequenceScape barcode prefix string>,
                 barcode          => <SequenceScape barcode integer>,
                 map              => <SequenceScape well address string
                                      without 0-pad e.g A1> }
  Returntype : hashref
  Caller     : general

=cut

sub find_infinium_plate {
  my ($self, $plate_name) = @_;

  my $dbh = $self->dbh;

  my $query =
    "SELECT
       sm.sanger_sample_id,
       sm.uuid,
       sm.gender,
       pl.barcode_prefix,
       pl.barcode,
       wl.map
     FROM
       current_plates pl, current_samples sm, current_wells wl
     WHERE
       pl.infinium_barcode = ?
       AND wl.plate_barcode = pl.barcode
       AND wl.sample_internal_id = sm.internal_id";

  my $sth = $dbh->prepare($query);
  $sth->execute($plate_name);

  my %plate;
  while (my $row = $sth->fetchrow_hashref) {
    $plate{$row->{map}} = $row;
  }

  return \%plate;
}

1;

__END__

=head1 NAME

WTSI::Genotyping::Database::Warehouse

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
