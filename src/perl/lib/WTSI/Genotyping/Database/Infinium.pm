use utf8;

package WTSI::Genotyping::Database::Infinium;

use strict;
use warnings;
use Carp;

use WTSI::Genotyping::Database;

our @ISA = qw(WTSI::Genotyping::Database);


=head2 find_project_chip_design

  Arg [1]    : string
  Example    : $db->find_project_chip_design('my project name')
  Description: Returns Infinium chip design (product name) for a project
               name
  Returntype : string
  Caller     : general

=cut

sub find_project_chip_design {
  my ($self, $project_name) = @_;

  my $dbh = $self->dbh;

  my $query =
    "SELECT DISTINCT
       pd.product_name
     FROM
       project project,
       item projecti,
       appvalue projectav,
       projectproduct pp,
       productdefinition pd
     WHERE
       projecti.item = ?
       AND project.itemid = projecti.itemid
       AND projecti.itemtypeid = projectav.appvalueid
       AND projectav.appvaluetype = 'Project'
       AND project.project_id = pp.project_id
       AND pp.product_definition_id = pd.product_definition_id";

  my $sth = $dbh->prepare($query);
  my $rc = $sth->execute($project_name);

  my @chip_designs;
  while (my ($design) = $sth->fetchrow_array) {
    push(@chip_designs, $design);
  }

  unless (@chip_designs) {
    croak "No chip design was found for project '$project_name'\n";
  }

  if (scalar @chip_designs > 1) {
    croak ">1 chip design found for project '$project_name': [" .
      join(", ", @chip_designs) . "]\n";
  }

  return $chip_designs[0];
}


=head2 find_project_samples

  Arg [1]    : string
  Example    : $db->find_project_samples('my project name')
  Description: Returns sample details for a project. Each sample is returned
               as a hashref with the following keys and values:
               { plate    => <Infinium LIMS plate barcode string>,
                 well     => <well address string with 0-pad e.g A01>,
                 sample   => <sample name string>,
                 beadchip => <chip name string>,
                 status   => <status string of 'Pass' for passed samples>,
                 gtc_path => <path string of GTC format result file>,
                 idat_grn_path => <path string of IDAT format result file>,
                 idat_red_path => <path string of IDAT format result file> }
  Returntype : arrayref of hashrefs
  Caller     : general

=cut

sub find_project_samples {
  my ($self, $project_name) = @_;

  my $dbh = $self->dbh;

  my $query =
    "SELECT
         platei.item AS [plate],
         well.alpha_coordinate AS [well],
         samplei.item AS [sample],
         chipi.item AS [beadchip],
         statusav.appvalue AS [status],

         parentdir1.path + '\' + callappfile.file_name AS [gtc_path],
         parentdir2.path + '\' + redappfile.file_name AS [idat_red_path],
         parentdir3.path + '\' + greenappfile.file_name AS [idat_grn_path]

     FROM
        project project
        INNER JOIN item projecti
          ON project.itemid = projecti.itemid
        INNER JOIN appvalue projectav
          ON projecti.itemtypeid = projectav.appvalueid
        INNER JOIN sampleassignment sampleassn
          ON project.project_id = sampleassn.project_id
        INNER JOIN samplecontainer samplecon
          ON sampleassn.sample_container_id = samplecon.sample_container_id
        INNER JOIN item platei
          ON samplecon.itemid = platei.itemid
        INNER JOIN appvalue containerav
          ON platei.itemtypeid = containerav.appvalueid
        INNER JOIN samplewell well
          ON samplecon.sample_container_id = well.sample_container_id
        INNER JOIN sample sample
          ON well.sample_id = sample.sample_id
        INNER JOIN item samplei
          ON sample.itemid = samplei.itemid
        INNER JOIN appvalue sampleav
          ON samplei.itemtypeid = sampleav.appvalueid
        INNER JOIN samplebatchdetail samplebd
          ON project.project_id = samplebd.project_id
          AND well.sample_well_id = samplebd.sample_well_id
        INNER JOIN projectusage projectuse
          ON project.project_id = projectuse.project_id
          AND samplebd.sample_batch_detail_id = projectuse.sample_batch_detail_id
        INNER JOIN samplesection samplesect
          ON samplesect.sample_section_id = projectuse.sample_section_id
        INNER JOIN item sectioni
          ON sectioni.itemid = samplesect.itemid
        INNER JOIN appvalue sectionav
          ON sectioni.itemtypeid = sectionav.appvalueid
        INNER JOIN beadchip chip
          ON samplesect.bead_chip_id = chip.bead_chip_id
        INNER JOIN item chipi
          ON chip.itemid = chipi.itemid

        LEFT OUTER JOIN callfile callfile
          ON samplesect.sample_section_id = callfile.sample_section_id
        LEFT OUTER JOIN applicationfile callappfile
          ON callfile.application_file_id = callappfile.application_file_id
        LEFT OUTER JOIN parentdirectory parentdir1
          ON parentdir1.parent_directory_id = callappfile.parent_directory_id
        LEFT OUTER JOIN appvalue statusav
          ON callfile.status_id = statusav.appvalueid

        LEFT OUTER JOIN intensityfile redintensityfile
          ON redintensityfile.imaging_event_id = callfile.imaging_event_id
          AND redintensityfile.project_usage_id = projectuse.project_usage_id
        LEFT OUTER JOIN appvalue redchannelav
          ON redintensityfile.channel_id = redchannelav.appvalueid

        LEFT OUTER JOIN applicationfile redappfile
          ON redintensityfile.application_file_id = redappfile.application_file_id
        LEFT OUTER JOIN parentdirectory parentdir2
          ON parentdir2.parent_directory_id = redappfile.parent_directory_id

        LEFT OUTER JOIN intensityfile greenintensityfile
          ON greenintensityfile.imaging_event_id = callfile.imaging_event_id
         AND greenintensityfile.project_usage_id = projectuse.project_usage_id
        LEFT OUTER JOIN appvalue greenchannelav
          ON greenintensityfile.channel_id = greenchannelav.appvalueid

        LEFT OUTER JOIN applicationfile greenappfile
          ON greenintensityfile.application_file_id = greenappfile.application_file_id
        LEFT OUTER JOIN parentdirectory parentdir3
         ON parentdir3.parent_directory_id = greenappfile.parent_directory_id

      WHERE
         projecti.item = ?
         AND projectav.appvaluetype = 'Project'
	     AND sampleav.appvaluetype = 'Sample'
	     AND containerav.appvaluetype = 'SamplePlate'
	     AND sectionav.appvaluetype = 'SampleSection'

         AND (redchannelav.appvaluetype = 'Red'
              OR redchannelav.appvaluetype IS NULL)
         AND (greenchannelav.appvaluetype = 'Green'
              OR greenchannelav.appvaluetype IS NULL)

      ORDER BY
         platei.item,
         right(well.alpha_coordinate,2),
         well.alpha_coordinate";

  my $sth = $dbh->prepare($query);
  $sth->execute($project_name);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  unless (@samples) {
    croak "No samples were found for project '$project_name'\n";
  }

  return \@samples;
}

1;

__END__

=head1 NAME

WTSI::Genotyping::Database::Infinium

=head1 DESCRIPTION

A class for querying the Illumina Infinium LIMS database to retrieve
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
