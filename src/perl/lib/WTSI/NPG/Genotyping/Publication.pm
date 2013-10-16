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

use WTSI::NPG::Genotyping::Metadata qw($INFINIUM_PLATE_BARCODE_META_KEY
                                       $INFINIUM_PLATE_WELL_META_KEY
                                       $SEQUENOM_PLATE_NAME_META_KEY
                                       $SEQUENOM_PLATE_WELL_META_KEY
                                       $INFINIUM_SAMPLE_NAME
                                       infinium_fingerprint
                                       make_analysis_metadata
                                       make_infinium_metadata
                                       make_sequenom_metadata
                                       sequenom_fingerprint);

use WTSI::NPG::iRODS qw(
                        add_collection
                        add_collection_meta
                        add_object
                        add_object_meta
                        find_objects_by_meta
                        find_or_make_group
                        get_collection_meta
                        get_object_meta
                        group_exists
                        hash_path
                        list_collection
                        list_object
                        make_group_name
                        meta_exists
                        put_collection
                        set_group_access
                        validate_checksum_metadata);

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

use WTSI::NPG::Utilities qw(trim);

use base 'Exporter';
our @EXPORT_OK = qw(
                    $DEFAULT_SAMPLE_ARCHIVE
                    export_sequenom_files
                    parse_fluidigm_table
                    publish_idat_files
                    publish_gtc_files
                    publish_sequenom_files
                    publish_analysis_directory
                    update_infinium_metadata
                    update_sequenom_metadata
);


# This is the collection will be searched for sample data to cross
# reference with an analysis
our $DEFAULT_SAMPLE_ARCHIVE = '/archive/GAPI/gen/infinium';

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
      foreach my $file ($red, $grn) {
        eval {
          my $data_object =
            publish_infinium_file($file, $creator_uri, $publish_dest,
                                  $publisher_uri, $if_sample, $ssdb, $time);
          ++$published;
        };

        if ($@) {
          $log->error("Failed to publish '$file' to '$publish_dest': ", $@);
        }
        else {
          $log->info("Published '$file': $published of $total");
        }
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
        my $data_object =
          publish_infinium_file($file, $creator_uri, $publish_dest,
                                $publisher_uri, $if_sample, $ssdb, $time);
        ++$published;
      };

      if ($@) {
        $log->error("Failed to publish '$file' to '$publish_dest': ", $@);
      }
      else {
        $log->info("Published '$file': $published of $total");
      }
    }
    else {
      $log->warn("Failed to find the sample for '$file' in the Infinium LIMS");
    }
  }

  $log->info("Published $published/$total GTC files to '$publish_dest'");

  return $published;
}

sub publish_infinium_file {
  my ($file, $creator_uri, $publish_dest, $publisher_uri,
      $if_sample, $ssdb, $time) = @_;

  my $ss_sample =
    $ssdb->find_infinium_sample_by_plate($if_sample->{'plate'},
                                         $if_sample->{'well'});
  my @meta;
  push(@meta, make_infinium_metadata($if_sample));

  # TODO: decouple Sequencescape interaction from publication
  push(@meta, make_sample_metadata($ss_sample, $ssdb));

  my @fingerprint = infinium_fingerprint(@meta);
  my $data_object = publish_file($file, \@fingerprint,
                                 $creator_uri->as_string, $publish_dest,
                                 $publisher_uri->as_string, $time);
  return $data_object;
}

sub update_infinium_metadata {
  my ($data_object, $ssdb) = @_;

  my %current_meta = get_object_meta($data_object);
  my $infinium_barcode =
    get_single_metadata_value($data_object,
                              $INFINIUM_PLATE_BARCODE_META_KEY,
                              %current_meta);
  my $well = get_single_metadata_value($data_object,
                                       $INFINIUM_PLATE_WELL_META_KEY,
                                       %current_meta);
  $log->debug("Found plate well '$infinium_barcode': '$well' in ",
              "current metadata of '$data_object'");

  my $ss_sample =
    $ssdb->find_infinium_sample_by_plate($infinium_barcode, $well);

  unless ($ss_sample) {
    $log->logconfess("Failed to update metadata for '$data_object': ",
                     "failed to find sample in '$infinium_barcode' ",
                     "well '$well'");
  }

  $log->info("Updating metadata for '$data_object' from plate ",
             "'$infinium_barcode' well '$well'");

  my @meta = make_sample_metadata($ss_sample);
  update_object_meta($data_object, \@meta);

  my @groups = expected_irods_groups(@meta);
  grant_group_access($data_object, 'read', @groups);

  return $data_object;
}

=head2 publish_sequenom_files

  Arg [1]    : Sequenom plate hashref
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : DateTime object of publication

  Example    : my $n = publish_sequenom_files($plate1, $creator_uri,
                                              '/my/project', $publisher_uri,
                                              $now);
  Description: Publishes Sequenom CSV files to iRODS with attendant metadata.
               Republishes any file that is already published, but whose
               checksum has changed. One file is created for each well that
               has been analysed. The plate hashref is created by
               WTSI::NPG::Genotyping::Database::Sequenom::find_plate_results
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_sequenom_files {
  my ($plate, $creator_uri, $publish_dest, $publisher_uri, $time) = @_;

  my $total = scalar keys %$plate;
  my $published = 0;

  $log->debug("Publishing $total Sequenom CSV data files");

  my $tmpdir = tempdir(CLEANUP => 1);
  my $current_file;
  my $plate_name;

  foreach my $key (sort keys %$plate) {
    eval {
      my @records = @{$plate->{$key}};
      my $first = $records[0];
      my @keys = sort keys %$first;

      $plate_name = $first->{plate};
      my $file = sprintf("%s/%s_%s.csv", $tmpdir,
                         $plate_name, $first->{well});
      $current_file = $file;

      my $record_count = write_sequenom_csv_file($file, \@keys, \@records);
      $log->debug("Wrote $record_count records into $file");

      my @meta = make_sequenom_metadata($first);
      my @fingerprint = sequenom_fingerprint(@meta);
      my $data_object = publish_file($file, \@fingerprint,
                                     $creator_uri, $publish_dest,
                                     $publisher_uri, $time);
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

  $log->info("Published $published/$total CSV files for '$plate_name' ",
             "to '$publish_dest'");

  return $published;
}

sub export_sequenom_files {
  my ($plate, $export_dest) = @_;

  my $total = scalar keys %$plate;
  my $exported = 0;

  $log->debug("Exporting $total Sequenom CSV data files");

  my $current_file;
  my $plate_name;

  foreach my $key (sort keys %$plate) {
    eval {
      my @records = @{$plate->{$key}};
      my $first = $records[0];
      my @keys = sort keys %$first;

      $plate_name = $first->{plate};
      my $file = sprintf("%s/%s_%s.csv", $export_dest,
                         $plate_name, $first->{well});
      $current_file = $file;

      my $record_count = write_sequenom_csv_file($file, \@keys, \@records);
      $log->debug("Wrote $record_count records into $file");

      ++$exported;
    };

    if ($@) {
      $log->error("Failed to export '$current_file' to ",
                  "'$export_dest': ", $@);
    }
    else {
      $log->debug("Exported '$current_file': $exported of $total");
    }
  }

  $log->info("Exported $exported/$total CSV files for '$plate_name' ",
             "to '$export_dest'");

  return $exported;
}

sub update_sequenom_metadata {
  my ($data_object, $snpdb, $ssdb) = @_;

  my %current_meta = get_object_meta($data_object);
  my $plate_name = get_single_metadata_value($data_object,
                                             $SEQUENOM_PLATE_NAME_META_KEY,
                                             %current_meta);
  my $well = get_single_metadata_value($data_object,
                                       $SEQUENOM_PLATE_WELL_META_KEY,
                                       %current_meta);
  $log->debug("Found plate well '$plate_name': '$well' in ",
              "current metadata of '$data_object'");

  # Identify the plate via the SNP database.  It would be preferable
  # to look up directly in the warehouse.  However, the warehouse does
  # not contain tracking information on Sequenom plates
  my $plate_id = $snpdb->find_sequenom_plate_id($plate_name);
  if (defined $plate_id) {
    $log->debug("Found Sequencescape plate identifier '$plate_id' for ",
                "'$data_object'");

    my $ss_sample = $ssdb->find_sample_by_plate($plate_id, $well);
    unless ($ss_sample) {
      $log->logconfess("Failed to update metadata for '$data_object': ",
                       "failed to find sample in '$plate_name' ",
                       "well '$well'");
    }

    $log->info("Updating metadata for '$data_object' from plate ",
               "'$plate_name' well '$well'");

    my @meta = make_sample_metadata($ss_sample);
    update_object_meta($data_object, \@meta);

    my @groups = expected_irods_groups(@meta);
    grant_group_access($data_object, 'read', @groups);
  }
  else {
    $log->info("Skipping update of metadata for '$data_object': ",
               "plate name '$plate_name' is not present in SNP database");
  }

  return $data_object;
}

=head2 publish_analysis_directory

  Arg [1]    : directory containing the analysis results
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : genotyping pipeline database handle
  Arg [6]    : pipeline run name in pipeline database
  Arg [7]    : sample data archive used to search for sample data
               to cross reference
  Arg [7]    : DateTime object of publication

  Example    : my $uuid =
                   publish_analysis_directory($dir, $creator_uri,
                                             '/my/project', $publisher_uri,
                                             $pipedb, 'run1',
                                             '/archive/GAPI/gen/infinium',
                                             $now);
  Description: Publishes an analysis directory to an iRODS collection. Adds
               to the collection metadata describing the genotyping projects
               analysed and a new UUID for the analysis. It also locates the
               corresponding sample data in iRODS and cross-references it to
               the analysis by adding the UUID to the sample metadata. It also
               adds the sample study/studies of the samples to the analysis
               collection metadata.
  Returntype : a new UUID for the analysis or undef on failure
  Caller     : general

=cut

sub publish_analysis_directory {
  my ($dir, $creator_uri, $publish_dest, $publisher_uri, $pipedb, $run_name,
      $sample_archive, $time) = @_;

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
  my $analysis_uuid;
  my $num_projects = 0;
  my $num_samples = 0;
  my $num_objects = 0;

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
    $analysis_uuid = $uuid_meta[0]->[1];

    foreach my $title (@project_titles) {
      # Find the samples included at the analysis stage
      my %included_samples =
        make_included_sample_table($title, $pipedb, $run_name);
      my $num_included = scalar keys %included_samples;

      if ($num_included == 0) {
        $log->logcroak("There were no samples marked for inclusion in the ",
                       "pipeline database. Aborting.");
      }

      my %studies_seen;

      foreach my $included_sample_name (sort keys %included_samples) {
        my @conds = (['dcterms:title' => $title],
                     [$INFINIUM_SAMPLE_NAME => $included_sample_name]);

        my @data_objects = find_objects_by_meta($sample_archive, @conds);
        unless (@data_objects) {
          $log->logconfess("Failed to find data in iRODS for sample ",
                           "'$included_sample_name' in project '$title'");
        }

        # Should be triplets of 1x gtc plus 2x idat files for each sample
        foreach my $data_object (@data_objects) {
          # Xref analysis to sample studies
          my %sample_meta = get_object_meta($data_object);

          if (exists $sample_meta{$STUDY_ID_META_KEY}) {
            my @sample_studies = @{$sample_meta{$STUDY_ID_META_KEY}};
            $log->debug("Sample '$included_sample_name' has metadata for ",
                        "studies [", join(", ", @sample_studies), "]");

            foreach my $sample_study (@sample_studies) {
              unless (exists $studies_seen{$sample_study}) {
                push(@analysis_meta, [$STUDY_ID_META_KEY => $sample_study]);
                $studies_seen{$sample_study}++;
              }
            }
          }
          else {
            $log->logconfess("Failed to find a study_id in iRODS for sample ",
                             "'$included_sample_name' data object ",
                             "'$data_object' in project '$title'");
          }

          # Xref samples to analysis UUID
          update_object_meta($data_object, \@uuid_meta);
          ++$num_objects;
        }

        ++$num_samples;

        $log->info("Cross-referenced $num_samples/$num_included samples ",
                   "in project '$title'")
      }

      ++$num_projects;
    }

    update_collection_meta($analysis_coll, \@analysis_meta);

    my @groups = expected_irods_groups(@analysis_meta);
    grant_group_access($analysis_coll, '-r read', @groups);
  };

  if ($@) {
    $log->error("Failed to publish: ", $@);
    undef $analysis_uuid;
  }
  else {
    $log->info("Published '$dir' to '$analysis_coll' and ",
               "cross-referenced $num_objects data objects in ",
               "for $num_samples samples in $num_projects projects");
  }

  return $analysis_uuid;
}


# Find samples marked as excluded during the analysis, keyed by their
# name in the Infinium LIMS
sub make_included_sample_table {
  my ($project_title, $pipedb, $run_name) = @_;

  my %sample_table;

  my @samples = $pipedb->sample->search
    ({'piperun.name' => $run_name,
      'dataset.if_project' => $project_title,
      'me.include' => 1},
     {join => {dataset => 'piperun'}});

  foreach my $sample (@samples) {
    $sample_table{$sample->name} = $sample;
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

sub parse_fluidigm_table {
  my ($fh) = @_;
  binmode($fh, ':utf8');

  # True if we are in the header lines from 'Chip Run Info' to 'Allele
  # Axis Mapping' inclusive
  my $in_header = 0;
  # True if we are in the unique column names row above the sample
  # block
  my $in_column_names = 0;
  # True if we are past the header and into a data block
  my $in_sample_block = 0;

  # Arrays of sample data lines keyed on Chamber IDs
  my %sample_data;

  # For error reporting
  my $line_num = 0;
  my $expected_num_columns = 12;
  my $num_sample_rows = 0;

  my @header;
  my @column_names;

  while (my $line = <$fh>) {
    ++$line_num;
    chomp($line);
    next if $line =~ m/^\s*$/;

    if ($line =~ /^Chip Run Info/) { $in_header = 1; }
    if ($line =~ /^Experiment/)    { $in_header = 0; }
    if ($line =~ /^ID/)            { $in_column_names = 1; }
    if ($line =~ /^S[0-9]+\-[A-Z][0-9]+/) {
      $in_column_names = 0;
      $in_sample_block = 1;
    }

    if ($in_header) {
      push(@header, $line);
      next;
    }

    if ($in_column_names) {
      @column_names = map { trim($_) } split(',', $line);
      my $num_columns = scalar @column_names;
      unless ($num_columns == $expected_num_columns) {
        $log->logconfess("Parse error: expected $expected_num_columns ",
                         "columns, but found $num_columns at line $line_num");
      }
      next;
    }

    if ($in_sample_block) {
      my @columns = map { trim($_) } split(',', $line);
      my $num_columns = scalar @columns;
      unless ($num_columns == $expected_num_columns) {
        $log->logconfess("Parse error: expected $expected_num_columns ",
                         "columns, but found $num_columns at line $line_num");
      }

      my $id = $columns[0];
      my ($sample_address, $assay_num) = split('-', $id);
      unless ($sample_address) {
        $log->logconfess("Parse error: no sample address in '$id' ",
                         "at line $line_num");
      }
      unless ($assay_num) {
        $log->logconfess("Parse error: no assay number in '$id' ",
                         "at line $line_num");
      }

      if (! exists $sample_data{$sample_address}) {
        $sample_data{$sample_address} = [];
      }

      push(@{$sample_data{$sample_address}}, \@columns);
      $num_sample_rows++;
      next;
    }
  }

  unless (@header) {
    $log->logconfess("Parse error: no header rows found");
  }
  unless (@column_names) {
    $log->logconfess("Parse error: no column names found");
  }

  if ($num_sample_rows == (96 * 96)) {
    unless (scalar keys %sample_data == 96) {
      $log->logconfess("Parse error: expected data for 96 samples, found ",
                       scalar keys %sample_data);
    }
  }
  elsif ($num_sample_rows == (192 * 24)) {
    unless (scalar keys %sample_data == 192) {
      $log->logconfess("Parse error: expected data for 192 samples, found ",
                       scalar keys %sample_data);
    }
  }
  else {
    $log->logconfess("Parse error: expected ", 96 * 96, " or ", 192 * 24,
                     " sample data rows, found $num_sample_rows");
  }

  return (\@header, \@column_names, \%sample_data);
}


sub get_single_metadata_value {
  my ($target, $key, %meta) = @_;

  unless (exists $meta{$key}) {
    $log->logconfess("Failed to update metadata for '$target': ",
                     "key '$key' is missing from current metadata");
  }

  my @values = @{$meta{$key}};
   if (scalar @values > 1) {
    $log->logconfess("Invalid metadata on '$target': key '$key'",
                     "has >1 value in current metadata: [",
                     join(', ', @values), "]");
  }

  return shift @values;
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
