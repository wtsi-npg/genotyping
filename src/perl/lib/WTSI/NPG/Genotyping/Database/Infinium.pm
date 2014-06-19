use utf8;

package WTSI::NPG::Genotyping::Database::Infinium;

use Carp;
use Moose;

extends 'WTSI::NPG::Database';

with 'WTSI::NPG::Cacheable';

# Method names for MOP operations
our $FIND_PROJECT_CHIP_DESIGN   = 'find_project_chip_design';
our $IS_METHYLATION_CHIP_DESIGN = 'is_methylation_chip_design';
our $FIND_SCANNED_SAMPLE        = 'find_scanned_sample';
our $FIND_CALLED_SAMPLE         = 'find_called_sample';

our $COMMON_SAMPLE_REPORT =
  qq(projecti.item             AS [project],
     platei.item               AS [plate],
     well.alpha_coordinate     AS [well],
     samplei.item              AS [sample],
     chipi.item                AS [beadchip],
     samplesectd.section_label AS [beadchip_section],
     productd.product_name     AS [beadchip_design],
     productr.product_revision AS [beadchip_revision],
     statusav.appvalue         AS [status],
     callparentdir.path  + '\\' + callappfile.file_name  AS [gtc_path],
     redparentdir.path   + '\\' + redappfile.file_name   AS [idat_red_path],
     greenparentdir.path + '\\' + greenappfile.file_name AS [idat_grn_path]);

our $COMMON_SAMPLE_SELECT =
  qq(project project
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
     INNER JOIN samplesectiondefinition samplesectd
       ON samplesect.sample_section_definition_id = samplesectd.sample_section_definition_id
     INNER JOIN item sectioni
       ON sectioni.itemid = samplesect.itemid

     INNER JOIN appvalue sectionav
       ON sectioni.itemtypeid = sectionav.appvalueid
     INNER JOIN beadchip chip
       ON samplesect.bead_chip_id = chip.bead_chip_id
     INNER JOIN item chipi
       ON chip.itemid = chipi.itemid

     INNER JOIN productrevision productr
       ON samplesectd.product_revision_id = productr.product_revision_id
     INNER JOIN productdefinition productd
       ON productr.product_definition_id = productd.product_definition_id);

my $meta = __PACKAGE__->meta;

around $FIND_PROJECT_CHIP_DESIGN => sub {
  my ($orig, $self, $project_title) = @_;

 defined $project_title or
    $self->logconfess('A defined project_title argument is required');
  $project_title or
    $self->logconfess('A non-empty project_title argument is required');

  my $cache = $self->get_method_cache
    ($meta->get_method($FIND_PROJECT_CHIP_DESIGN), {default_expires_in => 600});
  my $key = $project_title;

  return $self->get_with_cache($cache, $key, $orig, $project_title);
};

=head2 find_project_chip_design

  Arg [1]    : string
  Example    : $db->find_project_chip_design('my project name')
  Description: Returns Infinium chip design (product name) for a project
               name
  Returntype : ArrayRef of strings

=cut

sub find_project_chip_design {
  my ($self, $project_title) = @_;

  defined $project_title or
    $self->logconfess('A defined project_title argument is required');
  $project_title or
    $self->logconfess('A non-empty project_title argument is required');

  my $query =
    qq(SELECT DISTINCT
         pd.product_name
       FROM
         project project,
         item projecti,
         appvalue projectav,
         projectproduct pp,
         productdefinition pd
       WHERE
         projecti.item                = ?
         AND project.itemid           = projecti.itemid
         AND projecti.itemtypeid      = projectav.appvalueid
         AND projectav.appvaluetype   = 'Project'
         AND project.project_id       = pp.project_id
         AND pp.product_definition_id = pd.product_definition_id);

  $self->trace("Executing: '$query' with args [$project_title]");
  my $sth = $self->dbh->prepare($query);
  my $rc = $sth->execute($project_title);

  my @chip_designs;
  while (my ($design) = $sth->fetchrow_array) {
    push(@chip_designs, $design);
  }

  unless (@chip_designs) {
    $self->logconfess("No chip design was found for project '$project_title'");
  }

  return \@chip_designs;
}

around $IS_METHYLATION_CHIP_DESIGN => sub {
  my ($orig, $self, $chip_design) = @_;

 defined $chip_design or
    $self->logconfess('A defined chip_design argument is required');
  $chip_design or
    $self->logconfess('A non-empty chip_design argument is required');

  my $cache = $self->get_method_cache
    ($meta->get_method($IS_METHYLATION_CHIP_DESIGN),
     {default_expires_in => 600});
  my $key = $chip_design;

  return $self->get_with_cache($cache, $key, $orig, $chip_design);
};

sub is_methylation_chip_design {
  my ($self, $chip_design) = @_;

 defined $chip_design or
   $self->logconfess('A defined chip_design argument is required');
  $chip_design or
    $self->logconfess('A non-empty chip_design argument is required');

  my $query =
    qq(SELECT
         pl.name
       FROM
         productdefinition pd,
         productline       pl
       WHERE
         pd.product_name        = ?
         AND pd.product_line_id = pl.product_line_id);

  $self->trace("Executing: '$query' with args [$chip_design]");
  my $sth = $self->dbh->prepare($query);
  my $rc = $sth->execute($chip_design);

  my @lines;
  while (my ($name) = $sth->fetchrow_array) {
    push(@lines, $name);
  }

  unless (@lines) {
    $self->logconfess("Failed to find any product lines matching ",
                      "chip design '$chip_design'");
  }

  return (grep { $_ eq 'Infinium HD Methylation' } @lines) ? 1 : 0
}

=head2 find_project_samples

  Arg [1]    : string
  Example    : $db->find_project_samples('my project name')
  Description: Returns sample details for a project. Each sample is returned
               as a hashref with the following keys and values:
               { plate             => <Infinium LIMS plate barcode string>,
                 well              => <well address string with 0-pad e.g A01>,
                 sample            => <sample name string>,
                 beadchip          => <chip name string>,
                 beadchip_section  => <chip section name>,
                 beadchip_design   => <chip design name>,
                 beadchip_revision => <chip revision name>,
                 status            => <status string of 'Pass' for passed
                                       samples>,
                 gtc_path          => <path string of GTC format result file>,
                 idat_grn_path     => <path string of IDAT format result file>,
                 idat_red_path     => <path string of IDAT format result file> }
  Returntype : arrayref of hashrefs

=cut

sub find_project_samples {
  my ($self, $project_title) = @_;

  defined $project_title or
    $self->logconfess('The project_title argument was undefined');

  my $query =
    qq(SELECT DISTINCT
         projecti.item                  AS [project],
         platei.item                    AS [plate],
         left(well.alpha_coordinate,1)  AS [plate_row],
         right(well.alpha_coordinate,2) AS [plate_column],
         well.alpha_coordinate          AS [well],
         samplei.item                   AS [sample],
         chipi.item                     AS [beadchip],
         samplesectd.section_label      AS [beadchip_section],
         productd.product_name          AS [beadchip_design],
         productr.product_revision      AS [beadchip_revision],
         statusav.appvalue              AS [status],
         callparentdir.path  + '\\' + callappfile.file_name  AS [gtc_path],
         redparentdir.path   + '\\' + redappfile.file_name   AS [idat_red_path],
         greenparentdir.path + '\\' + greenappfile.file_name AS [idat_grn_path],
         pcontent.latest_image_date                          AS [image_date],
         CONVERT(CHAR(23), pcontent.latest_image_date, 127)  AS [image_iso_date]) . q{ } .

       'FROM' . q{ } . $COMMON_SAMPLE_SELECT . q{ } .

    qq(LEFT OUTER JOIN intensityfile redintensityfile
         ON redintensityfile.project_usage_id = projectuse.project_usage_id
       LEFT OUTER JOIN appvalue redchannelav
         ON redintensityfile.channel_id = redchannelav.appvalueid

       LEFT OUTER JOIN applicationfile redappfile
         ON redintensityfile.application_file_id = redappfile.application_file_id
       LEFT OUTER JOIN parentdirectory redparentdir
         ON redparentdir.parent_directory_id = redappfile.parent_directory_id

       LEFT OUTER JOIN intensityfile greenintensityfile
         ON greenintensityfile.project_usage_id = projectuse.project_usage_id
       LEFT OUTER JOIN appvalue greenchannelav
         ON greenintensityfile.channel_id = greenchannelav.appvalueid

       LEFT OUTER JOIN applicationfile greenappfile
         ON greenintensityfile.application_file_id = greenappfile.application_file_id
       LEFT OUTER JOIN parentdirectory greenparentdir
        ON greenparentdir.parent_directory_id = greenappfile.parent_directory_id

      LEFT OUTER JOIN imagingevent
         ON imagingevent.imaging_event_id = redintensityfile.imaging_event_id
         AND imagingevent.imaging_event_id = greenintensityfile.imaging_event_id

       LEFT OUTER JOIN callfile callfile
         ON samplesect.sample_section_id = callfile.sample_section_id
         AND imagingevent.imaging_event_id = callfile.imaging_event_id

       LEFT OUTER JOIN applicationfile callappfile
         ON callfile.application_file_id = callappfile.application_file_id
       LEFT OUTER JOIN parentdirectory callparentdir
         ON callparentdir.parent_directory_id = callappfile.parent_directory_id
       LEFT OUTER JOIN appvalue statusav
         ON callfile.status_id = statusav.appvalueid

       LEFT OUTER JOIN projectcontent_wggt pcontent
         ON pcontent.latest_image_tranid = imagingevent.tranid

     WHERE
        projecti.item                    = ?
        AND projectav.appvaluetype       = 'Project'
        AND sampleav.appvaluetype        = 'Sample'
        AND containerav.appvaluetype     = 'SamplePlate'
        AND sectionav.appvaluetype       = 'SampleSection'

        AND ((redchannelav.appvaluetype       = 'Red'
              AND greenchannelav.appvaluetype = 'Green'
              AND imagingevent.imaging_event_id IS NOT NULL)
              OR
              (redchannelav.appvaluetype       IS NULL
               AND greenchannelav.appvaluetype IS NULL
               AND callappfile.file_name       IS NULL))

     ORDER BY
        platei.item,
        plate_row,
        plate_column,
        pcontent.latest_image_date DESC);

  # Note: The clauses
  #
  # ((redchannelav.appvaluetype       = 'Red'
  #   AND greenchannelav.appvaluetype = 'Green'
  #   AND imagingevent.imaging_event_id IS NOT NULL)
  #   OR
  #   (redchannelav.appvaluetype       IS NULL
  #    AND greenchannelav.appvaluetype IS NULL
  #    AND callappfile.file_name       IS NULL))
  #
  # are present to ensure that samples that were never scanned are
  # captured; the outer joins alone are not sufficient. Such samples
  # may not have an imaging event associated with them.

  $self->trace("Executing: '$query' with args [$project_title]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($project_title);

  # There isn't a good way of selecting the latest idat files using a
  # database query. Where there have been re-scans we find the latest
  # by ordering the results by descending scan date and returning only
  # the first row we encounter for each sample.

  my %samples;
  my @samples;

  while (my $row = $sth->fetchrow_hashref) {
    my $sample = $row->{sample};
    my $date = $row->{image_iso_date};

    unless (exists $samples{$sample}) {
      $samples{$sample} = [];
    }

    push @{$samples{$sample}}, $row;
  }

  $self->debug("Found ", scalar keys %samples,
               " samples in project '$project_title'");

  foreach my $sample (keys %samples) {
    my @elts = @{$samples{$sample}};
    my $num_elts = scalar @elts;
    if ($num_elts > 1) {
      $self->info("Sample '$sample' has $num_elts scans");

      foreach my $elt (@elts) {
        unless (defined $elt->{image_iso_date}) {
          $self->logconfess("Unable to choose which scan record to use for ",
                            "sample '$sample' because there is no scan date ",
                            "recorded and there are $num_elts scans");
        }
      }

      @elts = sort { $b->{image_iso_date} cmp $a->{image_iso_date} } @elts;

      $self->warn("For sample '$sample' selecting latest scan dated ",
                  $elts[0]->{image_iso_date}, " from $num_elts scans");
    }

    push @samples, $elts[0];
  }

  unless (scalar @samples) {
    $self->logconfess("No samples were found for project '$project_title'");
  }

  return \@samples;
}

around $FIND_SCANNED_SAMPLE => sub {
  my ($orig, $self, $filename) = @_;

  defined $filename or
    $self->logconfess('A defined filename argument is required');
  $filename or
    $self->logconfess('A non-empty filename argument is required');

  my $cache = $self->get_method_cache
    ($meta->get_method($FIND_SCANNED_SAMPLE), {default_expires_in => 600});
  my $key = $filename;

  return $self->get_with_cache($cache, $key, $orig, $filename);
};

=head2 find_scanned_sample

  Arg [1]    : string
  Example    : $db->find_scanned_sample('<red idat filename>')
  Description: Returns sample details for a specific intensity file (red
               channel, arbitrarily). The sample is returned as a
               hashref with the following keys and values:
               { project           => <Infinium LIMS genotyping project title>,
                 plate             => <Infinium LIMS plate barcode string>,
                 well              => <well address string with 0-pad e.g A01>,
                 sample            => <sample name string>,
                 beadchip          => <chip name string>,
                 beadchip_section  => <chip section name>,
                 beadchip_design   => <chip design name>,
                 beadchip_revision => <chip revision name>,
                 status            => <status string of 'Pass' for passed samples>,
                 gtc_path          => <path string of GTC format result file>,
                 idat_grn_path     => <path string of IDAT format result file>,
                 idat_red_path     => <path string of IDAT format result file> }
  Returntype : hashref

=cut

sub find_scanned_sample {
  my ($self, $filename) = @_;

  defined $filename or
    $self->logconfess('A defined filename argument is required');
  $filename or
    $self->logconfess('A non-empty filename argument is required');

  my $query =
    'SELECT' . q{ } . $COMMON_SAMPLE_REPORT . q{ } .
    'FROM'   . q{ } . $COMMON_SAMPLE_SELECT . q{ } .

      qq(LEFT OUTER JOIN intensityfile redintensityfile
           ON redintensityfile.project_usage_id = projectuse.project_usage_id
         LEFT OUTER JOIN appvalue redchannelav
           ON redintensityfile.channel_id = redchannelav.appvalueid

         LEFT OUTER JOIN applicationfile redappfile
           ON redintensityfile.application_file_id = redappfile.application_file_id
         LEFT OUTER JOIN parentdirectory redparentdir
           ON redparentdir.parent_directory_id = redappfile.parent_directory_id

         LEFT OUTER JOIN intensityfile greenintensityfile
           ON greenintensityfile.project_usage_id = projectuse.project_usage_id
         LEFT OUTER JOIN appvalue greenchannelav
           ON greenintensityfile.channel_id = greenchannelav.appvalueid

         LEFT OUTER JOIN applicationfile greenappfile
           ON greenintensityfile.application_file_id = greenappfile.application_file_id
         LEFT OUTER JOIN parentdirectory greenparentdir
          ON greenparentdir.parent_directory_id = greenappfile.parent_directory_id

         LEFT OUTER JOIN imagingevent
           ON imagingevent.imaging_event_id = redintensityfile.imaging_event_id
           AND imagingevent.imaging_event_id = greenintensityfile.imaging_event_id

         LEFT OUTER JOIN callfile callfile
           ON samplesect.sample_section_id = callfile.sample_section_id
           AND imagingevent.imaging_event_id = callfile.imaging_event_id

         LEFT OUTER JOIN applicationfile callappfile
           ON callfile.application_file_id = callappfile.application_file_id
         LEFT OUTER JOIN parentdirectory callparentdir
           ON callparentdir.parent_directory_id = callappfile.parent_directory_id
         LEFT OUTER JOIN appvalue statusav
           ON callfile.status_id = statusav.appvalueid

         WHERE
            projectav.appvaluetype          = 'Project'
	        AND sampleav.appvaluetype       = 'Sample'
	        AND containerav.appvaluetype    = 'SamplePlate'
	        AND sectionav.appvaluetype      = 'SampleSection'
            AND redappfile.file_name        = ?
            AND redchannelav.appvaluetype   = 'Red'
            AND greenchannelav.appvaluetype = 'Green'
            AND imagingevent.is_latest      = 1

         ORDER BY
            platei.item,
            right(well.alpha_coordinate,2),
            well.alpha_coordinate);

  $self->trace("Executing: '$query' with args [$filename]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($filename);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n samples were returned where 1 sample was expected.");
  }

  return shift @samples;
}

around $FIND_CALLED_SAMPLE => sub {
  my ($orig, $self, $filename) = @_;

  defined $filename or
    $self->logconfess('A defined filename argument is required');
  $filename or
    $self->logconfess('A non-empty filename argument is required');

  my $cache = $self->get_method_cache
    ($meta->get_method($FIND_CALLED_SAMPLE), {default_expires_in => 600});
  my $key = $filename;

  return $self->get_with_cache($cache, $key, $orig, $filename);
};

=head2 find_called_sample

  Arg [1]    : string
  Example    : $db->find_called_sample('<GTC filename>')
  Description: Returns sample details for a specific GTC file. The sample
               is returned as a hashref with the following keys and values:
               { project           => <Infinium LIMS genotyping project title>,
                 plate             => <Infinium LIMS plate barcode string>,
                 well              => <well address string with 0-pad e.g A01>,
                 sample            => <sample name string>,
                 beadchip          => <chip name string>,
                 beadchip_section  => <chip section name>,
                 beadchip_design   => <chip design name>,
                 beadchip_revision => <chip revision name>,
                 status            => <status string of 'Pass' for passed
                                       samples>,
                 gtc_path          => <path string of GTC format result file>,
                 idat_grn_path     => <path string of IDAT format result file>,
                 idat_red_path     => <path string of IDAT format result file> }
  Returntype : hashref

=cut

sub find_called_sample {
  my ($self, $filename) = @_;

  defined $filename or
    $self->logconfess('A defined filename argument is required');
  $filename or
    $self->logconfess('A non-empty filename argument is required');

  my $query =
    'SELECT' . q{ } . $COMMON_SAMPLE_REPORT . q{ } .
    'FROM'   . q{ } . $COMMON_SAMPLE_SELECT . q{ } .

      qq(INNER JOIN intensityfile redintensityfile
           ON redintensityfile.project_usage_id = projectuse.project_usage_id
         INNER JOIN appvalue redchannelav
           ON redintensityfile.channel_id = redchannelav.appvalueid

         INNER JOIN applicationfile redappfile
           ON redintensityfile.application_file_id = redappfile.application_file_id
         INNER JOIN parentdirectory redparentdir
           ON redparentdir.parent_directory_id = redappfile.parent_directory_id

         INNER JOIN intensityfile greenintensityfile
           ON greenintensityfile.project_usage_id = projectuse.project_usage_id
         INNER JOIN appvalue greenchannelav
           ON greenintensityfile.channel_id = greenchannelav.appvalueid

         INNER JOIN applicationfile greenappfile
           ON greenintensityfile.application_file_id = greenappfile.application_file_id
         INNER JOIN parentdirectory greenparentdir
           ON greenparentdir.parent_directory_id = greenappfile.parent_directory_id

         LEFT OUTER JOIN imagingevent
           ON imagingevent.imaging_event_id = redintensityfile.imaging_event_id
           AND imagingevent.imaging_event_id = greenintensityfile.imaging_event_id

         LEFT OUTER JOIN callfile callfile
           ON samplesect.sample_section_id = callfile.sample_section_id
           AND imagingevent.imaging_event_id = callfile.imaging_event_id

         LEFT OUTER JOIN applicationfile callappfile
           ON callfile.application_file_id = callappfile.application_file_id
         LEFT OUTER JOIN parentdirectory callparentdir
           ON callparentdir.parent_directory_id = callappfile.parent_directory_id
         LEFT OUTER JOIN appvalue statusav
           ON callfile.status_id = statusav.appvalueid

         WHERE
            projectav.appvaluetype          = 'Project'
	        AND sampleav.appvaluetype       = 'Sample'
	        AND containerav.appvaluetype    = 'SamplePlate'
	        AND sectionav.appvaluetype      = 'SampleSection'
            AND callappfile.file_name       = ?
            AND redchannelav.appvaluetype   = 'Red'
            AND greenchannelav.appvaluetype = 'Green'
            AND imagingevent.is_latest      = 1

         ORDER BY
            platei.item,
            right(well.alpha_coordinate,2),
            well.alpha_coordinate);

  $self->trace("Executing: '$query' with args [$filename]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($filename);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  my $n = scalar @samples;
  if ($n > 1) {
    $self->logconfess("$n samples were returned where 1 sample was expected.");
  }

  return shift @samples;
}

=head2 find_scanned_samples_by_date

  Arg [1]    : start DateTime
  Arg [2]    : end DateTime. Optional, defaults to start

  Example    : $db->find_scanned_samples_by_date($start_date, $end_date)
  Description: Returns samples that were scanned between start and end
               dates, inclusive. Only the date part of the DateTime
               arguments is significant; these arguments are cloned and
               truncated prior to comparison.

               The sample is returned as a hashref with the following keys
               and values:
               { project           => <Infinium LIMS genotyping project title>,
                 plate             => <Infinium LIMS plate barcode string>,
                 well              => <well address string with 0-pad e.g A01>,
                 sample            => <sample name string>,
                 beadchip          => <chip name string>,
                 beadchip_section  => <chip section name>,
                 beadchip_design   => <chip design name>,
                 beadchip_revision => <chip revision name>,
                 status            => <status string of 'Pass' for passed
                                       samples>,
                 gtc_path          => <path string of GTC format result file>,
                 idat_grn_path     => <path string of IDAT format result file>,
                 idat_red_path     => <path string of IDAT format result file> }
  Returntype : hashref

=cut

sub find_scanned_samples_by_date {
  my ($self, $start_date, $end_date) = @_;

  defined $start_date or
    $self->logconfess('The start_date argument was undefined');

  $end_date ||= $start_date;

  my $start = $start_date->clone->truncate(to => 'day');
  my $end = $end_date->clone->truncate(to => 'day');

  if ($start->compare($end) > 0) {
    $self->logconfess("Start date '$start' was after end date '$end'");
  }

  my $query =
    qq(SELECT DISTINCT
         projecti.item                  AS [project],
         platei.item                    AS [plate],
         well.alpha_coordinate          AS [well],
         left(well.alpha_coordinate,1)  AS [plate_row],
         right(well.alpha_coordinate,2) AS [plate_column],
         samplei.item                   AS [sample],
         chipi.item                     AS [beadchip],
         samplesectd.section_label      AS [beadchip_section],
         productd.product_name          AS [beadchip_design],
         productr.product_revision      AS [beadchip_revision],
         statusav.appvalue              AS [status],
         pcontent.latest_image_date     AS [latest_scan_date],
         callparentdir.path  + '\\' + callappfile.file_name  AS [gtc_path],
         redparentdir.path   + '\\' + redappfile.file_name   AS [idat_red_path],
         greenparentdir.path + '\\' + greenappfile.file_name AS [idat_grn_path]
       FROM) .

      q{ } .  $COMMON_SAMPLE_SELECT . q{ } .

      qq(INNER JOIN intensityfile redintensityfile
           ON redintensityfile.project_usage_id = projectuse.project_usage_id
         INNER JOIN appvalue redchannelav
           ON redintensityfile.channel_id = redchannelav.appvalueid

         INNER JOIN applicationfile redappfile
           ON redintensityfile.application_file_id = redappfile.application_file_id
         INNER JOIN parentdirectory redparentdir
           ON redparentdir.parent_directory_id = redappfile.parent_directory_id

         INNER JOIN intensityfile greenintensityfile
           ON greenintensityfile.project_usage_id = projectuse.project_usage_id
         INNER JOIN appvalue greenchannelav
           ON greenintensityfile.channel_id = greenchannelav.appvalueid

         INNER JOIN applicationfile greenappfile
           ON greenintensityfile.application_file_id = greenappfile.application_file_id
         INNER JOIN parentdirectory greenparentdir
          ON greenparentdir.parent_directory_id = greenappfile.parent_directory_id

         INNER JOIN imagingevent
           ON imagingevent.imaging_event_id = redintensityfile.imaging_event_id
           AND imagingevent.imaging_event_id = greenintensityfile.imaging_event_id
         INNER JOIN projectcontent_wggt pcontent
           ON imagingevent.tranid = pcontent.latest_image_tranid

         LEFT OUTER JOIN callfile callfile
           ON samplesect.sample_section_id = callfile.sample_section_id
           AND imagingevent.imaging_event_id = callfile.imaging_event_id

         LEFT OUTER JOIN applicationfile callappfile
           ON callfile.application_file_id = callappfile.application_file_id
         LEFT OUTER JOIN parentdirectory callparentdir
           ON callparentdir.parent_directory_id = callappfile.parent_directory_id
         LEFT OUTER JOIN appvalue statusav
           ON callfile.status_id = statusav.appvalueid

     WHERE
              projectav.appvaluetype      = 'Project'
	      AND sampleav.appvaluetype       = 'Sample'
	      AND containerav.appvaluetype    = 'SamplePlate'
	      AND sectionav.appvaluetype      = 'SampleSection'
          AND redchannelav.appvaluetype   = 'Red'
          AND greenchannelav.appvaluetype = 'Green'
          AND imagingevent.is_latest      = 1
          AND pcontent.latest_image_date  >= ?
          AND pcontent.latest_image_date  <= ?

       ORDER BY
          platei.item,
          left(well.alpha_coordinate,1),
          right(well.alpha_coordinate,2));

  my $s = $start->ymd('-');
  my $e = $end->ymd('-');
  $self->trace("Executing: '$query' with args [$s, $e]");
  my $sth = $self->dbh->prepare($query);
  $sth->execute($s, $e);

  my @samples;
  while (my $row = $sth->fetchrow_hashref) {
    push(@samples, $row);
  }

  return \@samples;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Database::Infinium

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
