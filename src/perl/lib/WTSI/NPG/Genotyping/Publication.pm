use utf8;

package WTSI::NPG::Genotyping::Publication;

use strict;
use warnings;
use Carp;
use File::Basename qw(basename fileparse);
use File::Temp qw(tempdir);
use Net::LDAP;
use Text::CSV;
use URI;

use WTSI::NPG::Genotyping::Metadata qw(make_analysis_metadata
                                       make_infinium_metadata
                                       make_sequenom_metadata
                                       infinium_fingerprint
                                       sequenom_fingerprint);

use WTSI::NPG::iRODS qw(make_group_name
                        group_exists
                        find_or_make_group
                        set_group_access
                        list_object
                        add_object
                        checksum_object
                        get_object_meta
                        add_object_meta
                        find_objects_by_meta
                        list_collection
                        add_collection
                        put_collection
                        get_collection_meta
                        add_collection_meta
                        meta_exists
                        hash_path);

use WTSI::NPG::Metadata qw($SAMPLE_NAME_META_KEY
                           $STUDY_ID_META_KEY
                           make_creation_metadata
                           make_sample_metadata);

use WTSI::NPG::Publication qw(pair_rg_channel_files
                              publish_file
                              update_object_meta
                              update_collection_meta
                              expected_irods_groups
                              grant_group_access);

use base 'Exporter';
our @EXPORT_OK = qw($DEFAULT_SAMPLE_ARCHIVE_PATTERN
                    publish_idat_files
                    publish_gtc_files
                    publish_sequenom_files
                    publish_analysis_directory);


# This is the collection will be searched for sample data to cross
# reference with an analysis
our $DEFAULT_SAMPLE_ARCHIVE_PATTERN = '/archive/GAPI/gen/infinium%';

our $log = Log::Log4perl->get_logger('npg.irods.publish');


=head2 publish_idat_files

  Arg [1]    : arrayref of IDAT file names
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : Infinium database handle
  Arg [6]    : SequenceScape Warehouse database handle
  Arg [7]    : DateTime object of publication

  Example    : my $n = publish_idat_files(\@files, $creator_uri,
                                          '/my/project', $publisher_uri,
                                          $ifdb, $ssdb, $now);
  Description: Publishes IDAT file pairs to iRODS with attendant metadata.
               Skips any files where consent is absent. Republishes any
               file that is already published, but whose checksum has
               changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_idat_files {
  my ($files, $creator_uri, $publish_dest, $publisher_uri,
      $ifdb, $ssdb, $time) = @_;

  my @paired = pair_rg_channel_files($files, 'idat');
  my $pairs = scalar @paired;
  my $total = $pairs * 2;
  my $published = 0;

  $log->debug("Publishing $pairs pairs of idat files");

  foreach my $pair (@paired) {
    my ($red) = grep { m{Red}msxi } @$pair;
    my ($grn) = grep { m{Grn}msxi } @$pair;

    my ($basename, $dir, $suffix) = fileparse($red);

    $log->debug("Finding the sample for '$red' in the Infinium LIMS");
    my $if_sample = $ifdb->find_scanned_sample($basename);

    if ($if_sample) {
      eval {
        my $ss_sample =
          $ssdb->find_infinium_sample_by_plate($if_sample->{'plate'},
                                               $if_sample->{'well'});
        my @meta;
        push(@meta, make_infinium_metadata($if_sample));
        push(@meta, make_sample_metadata($ss_sample, $ssdb));

        my @fingerprint = infinium_fingerprint(@meta);

        foreach my $file ($red, $grn) {
          my $object = publish_file($file, \@fingerprint,
                                    $creator_uri->as_string, $publish_dest,
                                    $publisher_uri->as_string, $time);
          my @groups = expected_irods_groups(@meta);
          grant_group_access($object, 'read', @groups);

          ++$published;
        }
      };

      if ($@) {
        $log->error("Failed to publish '$red' + '$grn': ", $@);
      }
      else {
        $log->debug("Published '$red' + '$grn': $published of $total");
      }
    }
    else {
     $log->warn("Failed to find the sample for '$red' in the Infinium LIMS");
    }
  }

  $log->info("Published $published/$total idat files to '$publish_dest'");

  return $published;
}


=head2 publish_gtc_files

  Arg [1]    : arrayref of GTC file names
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : Infinium database handle
  Arg [6]    : SequenceScape Warehouse database handle
  Arg [7]    : DateTime object of publication

  Example    : my $n = publish_gtc_files(\@files, $creator_uri,
                                         '/my/project', $publisher_uri,
                                         $ifdb, $ssdb, $now);
  Description: Publishes GTC files to iRODS with attendant metadata.
               Skips any files where consent is absent. Republishes any
               file that is already published, but whose checksum has
               changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_gtc_files {
  my ($files, $creator_uri, $publish_dest, $publisher_uri,
      $ifdb, $ssdb, $time) = @_;

  my $total = scalar @$files;
  my $published = 0;

  $log->debug("Publishing $total GTC files");

  foreach my $file (@$files) {
    my ($basename, $dir, $suffix) = fileparse($file);

    $log->debug("Finding the sample for '$file' in the Infinium LIMS");
    my $if_sample = $ifdb->find_called_sample($basename);

    if ($if_sample) {
      eval {
        my $ss_sample =
          $ssdb->find_infinium_sample_by_plate($if_sample->{'plate'},
                                               $if_sample->{'well'});

        my @meta;
        push(@meta, make_infinium_metadata($if_sample));
        push(@meta, make_sample_metadata($ss_sample, $ssdb));

        my @fingerprint = infinium_fingerprint(@meta);

        my $object = publish_file($file, \@fingerprint,
                                  $creator_uri->as_string, $publish_dest,
                                  $publisher_uri->as_string, $time);
        my @groups = expected_irods_groups(@meta);
        grant_group_access($object, 'read', @groups);

        ++$published;
      };

      if ($@) {
        $log->error("Failed to publish '$file' to '$publish_dest': ", $@);
      }
      else {
        $log->debug("Published '$file': $published of $total");
      }
    }
    else {
      $log->warn("Failed to find the sample for '$file' in the Infinium LIMS");
    }
  }

  $log->info("Published $published/$total GTC files to '$publish_dest'");

  return $published;
}

# TODO: refactor the above publish_idat_files and publish_tc_file to
# use this method
sub publish_infinium_file {
  my ($file, $creator_uri, $publish_dest, $publisher_uri,
      $if_sample, $ssdb, $time) = @_;

  my $object;

  eval {
    my $ss_sample =
      $ssdb->find_infinium_sample_by_plate($if_sample->{'plate'},
                                           $if_sample->{'well'});
    my @meta;
    push(@meta, make_infinium_metadata($if_sample));
    push(@meta, make_sample_metadata($ss_sample, $ssdb));

    my @fingerprint = infinium_fingerprint(@meta);

    $object = publish_file($file, \@fingerprint,
                           $creator_uri->as_string, $publish_dest,
                           $publisher_uri->as_string, $time);
    my @groups = expected_irods_groups(@meta);
    grant_group_access($object, 'read', @groups);
  };

  if ($@) {
    $log->error("Failed to publish '$file' to '$publish_dest': ", $@);
  }

  return $object;
}

=head2 publish_sequenom_files

  Arg [1]    : Sequenom plate name
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : DateTime object of publication

  Example    : my $n = publish_sequenom_files('plate1', $creator_uri,
                                              '/my/project', $publisher_uri,
                                              $now);
  Description: Publishes Sequenom CSV files to iRODS with attendant metadata.
               Republishes any file that is already published, but whose
               checksum has changed. One file is created for each well that
               has been analysed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_sequenom_files {
  my ($plate, $creator_uri, $publish_dest, $publisher_uri, $time) = @_;

  my $total = scalar keys %$plate;
  my $published = 0;

  $log->debug("Publishing $total CSV files");

  my $tmpdir = tempdir(CLEANUP => 1);
  my $current_file;

  foreach my $key (sort keys %$plate) {
    eval {
      my @records = @{$plate->{$key}};
      my $first = $records[0];
      my @keys = sort keys %$first;

      my $file = sprintf("%s/%s_%s.csv", $tmpdir,
                         $first->{plate}, $first->{well});
      $current_file = $file;

      my $record_count = write_sequenom_csv_file($file, \@keys, \@records);
      $log->debug("Wrote $record_count records into $file");

      my @meta = make_sequenom_metadata($first);
      my @fingerprint = sequenom_fingerprint(@meta);

      my $object = publish_file($file, \@fingerprint,
                                $creator_uri, $publish_dest,
                                $publisher_uri, $time);
      my @groups = expected_irods_groups(@meta);
      grant_group_access($object, 'read', @groups);

      unlink $file;
      ++$published;
    };

    if ($@) {
      $log->error("Failed to publish '$current_file' to ",
                  "'$publish_dest': ", $@);
    }
    else {
      $log->debug("Published '$current_file': $published of $total");
    }
  }

  $log->info("Published $published/$total CSV files for '$plate' ",
             "to '$publish_dest'");

  return $published;
}

=head2 publish_analysis_directory

  Arg [1]    : directory containing the analysis results
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : genotyping pipeline database handle
  Arg [6]    : pipeline run name in pipeline database
  Arg [7]    : sample data archive pattern used to search for sample data
               to cross reference
  Arg [7]    : DateTime object of publication

  Example    : my $uuid =
                   publish_analysis_directory($dir, $creator_uri,
                                             '/my/project', $publisher_uri,
                                             $pipedb, 'run1',
                                             '/archive/GAPI/gen/infinium%',
                                             $now);
  Description: Publishes an analysis directory to an iRODS collection. Adds
               to the collection metadata describing the genotyping projects
               analysed and a new UUID for the analysis. It also locates the
               corresponding sample data in iRODS and cross-references it to
               the analysis by adding the UUID to the sample metadata. It also
               adds the sample study/studies of the samples to the analysis
               collection metadata.
  Returntype : a new UUID for the analysis
  Caller     : general

=cut

sub publish_analysis_directory {
  my ($dir, $creator_uri, $publish_dest, $publisher_uri, $pipedb, $run_name,
      $sample_archive_pattern, $time) = @_;

  my $basename = fileparse($pipedb->dbfile);
  # Make a path based on the database file's MD5 to enable even distribution
  my $hash_path = hash_path($pipedb->dbfile);

  $publish_dest =~ s!/$!!;
  my $target = join('/', $publish_dest, $hash_path);
  my $leaf_collection = join('/', $target, basename($dir));

  if (list_collection($leaf_collection)) {
    $log->logcroak("An iRODS collection already exists at ",
                   "'$leaf_collection'. ",
                   "Please move or delete it before proceeding.");
  }

  $log->debug("Finding the project titles in the analysis database");

  my @project_titles;
  foreach my $dataset($pipedb->piperun->find({name => $run_name})->datasets) {
    push(@project_titles, $dataset->if_project);
  }

  unless (@project_titles) {
    $log->logcroak("The analysis database contained no data for ",
                   "run '$run_name'")
  }

  my $analysis_coll;
  my $uuid;
  my $num_projects = 0;
  my $num_samples = 0;

  eval {
    my @analysis_meta;
    push(@analysis_meta, make_analysis_metadata(\@project_titles));
    push(@analysis_meta, make_creation_metadata($creator_uri, $time,
                                                $publisher_uri));

    unless (list_collection($target)) {
      add_collection($target);
    }

    $analysis_coll = put_collection($dir, $target);
    $log->info("Created new collection $analysis_coll");

    my @uuid_meta = grep { $_->[0] =~ /uuid/ } @analysis_meta;
    $uuid = $uuid_meta[0]->[1];

    foreach my $title (@project_titles) {
      # Find the sample-level data for this genotyping project
      my @sample_data =  @{find_objects_by_meta($sample_archive_pattern,
                                                'dcterms:title',
                                                $title)};
      # Find the samples included at the analysis stage
      my %included_samples =
        make_included_sample_table($title, $pipedb, $run_name);

      if (@sample_data) {
        $log->info("Adding cross-reference metadata to ", scalar @sample_data,
                   " data objects in genotyping project '$title'");

        my %studies_seen;

      DATUM:
        foreach my $sample_datum (@sample_data) {
          # Xref analysis to sample studies
          my %sample_meta = get_object_meta($sample_datum);

          my @sanger_sample_id = @{$sample_meta{$SAMPLE_NAME_META_KEY}};
          unless (@sanger_sample_id) {
            $log->warn("Found no Sanger sample ID for '$sample_datum'");
            next DATUM;
          }
          unless (scalar @sanger_sample_id == 1) {
            $log->warn("Found multiple Sanger sample IDs for ",
                       "'$sample_datum': [", join(",", @sanger_sample_id), "]");
            next DATUM;
          }

          # Only Xref the sample is it was not excluded at the
          # analysis stage
          if (exists $included_samples{$sanger_sample_id[0]}) {
            my @sample_studies = @{$sample_meta{$STUDY_ID_META_KEY}};

            foreach my $sample_study (@sample_studies) {
              unless (exists $studies_seen{$sample_study}) {
                push(@analysis_meta, [$STUDY_ID_META_KEY => $sample_study]);
                $studies_seen{$sample_study}++;
              }
            }

            # Xref samples to analysis UUID
            update_object_meta($sample_datum, \@uuid_meta);
            ++$num_samples;
          }
          else {
            $log->info("Excluding sample '", $sanger_sample_id[0],
                       "' from this analysis");
          }
        }

        ++$num_projects;
      }
      else {
        $log->warn("Found no sample data objects in iRODS for '$title'");
      }
    }

    update_collection_meta($analysis_coll, \@analysis_meta);

    my @groups = expected_irods_groups(@analysis_meta);
    my $make_groups = 0;

    grant_group_access($analysis_coll, '-r read', @groups);
  };

  if ($@) {
    $log->error("Failed to publish: ", $@);
  }
  else {
    $log->info("Published '$dir' to '$analysis_coll' and ",
               "cross-referenced $num_samples data objects in ",
               "$num_projects projects");
  }

  return $uuid;
}

# Find samples marked as excluded during the analysis
sub make_included_sample_table {
  my ($project_title, $pipedb, $run_name) = @_;

  my %sample_table;

  my @samples = $pipedb->sample->search
    ({'piperun.name' => $run_name,
      'dataset.if_project' => $project_title,
      'me.include' => 1},
     {join => {dataset => 'piperun'}});

  foreach my $sample (@samples) {
    $sample_table{$sample->sanger_sample_id} = $sample;
  }

  return %sample_table;
}

# Write to a file subset of data in records that match keys
sub write_sequenom_csv_file {
  my ($file, $keys, $records) = @_;
  my $records_written = 0;

  # Transform to the required output headers
  my $fn = sub {
    my $x = shift;
    $x =~ '^WELL$'                    && return 'WELL_POSITION';
    $x =~ /^(ASSAY|GENOTYPE|SAMPLE)$/ && return $x . '_ID';
    return $x;
  };

  my @header = map { uc } @$keys;
  @header = map { $fn->($_) } @header;

  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});
  $csv->column_names(\@header);

  # Handle UTF8 because users can enter arbitrary plate names
  open(my $out, '>:encoding(utf8)', $file)
    or $log->logcroak("Failed to open Sequenom CSV file '$file'",
                      " for writing: $!");
  $csv->print($out, \@header)
    or $log->logcroak("Failed to write header [", join(", ", @header),
                      "] to '$file': ", $csv->error_diag);

  foreach my $record (@$records) {
    my @columns;
    foreach my $key (@$keys) {
      push(@columns, $record->{$key});
    }

    $csv->print($out, \@columns)
      or $log->logcroak("Failed to write record [", join(", ", @columns),
                        "] to '$file': ", $csv->error_diag);
    ++$records_written;
  }

  close($out);

  return $records_written;
}

1;

__END__

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
