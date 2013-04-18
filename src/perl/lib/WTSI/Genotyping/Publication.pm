use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;
use File::Basename qw(basename);
use Net::LDAP;
use URI;

use WTSI::Genotyping::iRODS qw(make_group_name
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
                               add_collection_meta
                               meta_exists
                               hash_path);

# This is the collection will be searched for sample data to cross
# reference with an analysis
our $SAMPLE_DATA_ARCHIVE_PATTERN = '/archive/GAPI/gen/infinium%';

our $log = Log::Log4perl->get_logger('npg.irods.publish');


=head2 get_wtsi_uri

  Example    : my $uri = get_wtsi_uri();
  Description: Returns the URI of the WTSI.
  Returntype : URI
  Caller     : general

=cut

sub get_wtsi_uri {
  my $uri = URI->new("http:");
  $uri->host('www.sanger.ac.uk');

  return $uri;
}


=head2 get_publisher_uri

  Arg [1]    : Login name string
  Example    : my $uri = get_publisher_uri($login);
  Description: Returns the LDAP URI of the user publishing data.
  Returntype : URI
  Caller     : general

=cut

sub get_publisher_uri {
  my ($uid) = @_;

  my $publisher_uri = URI->new("ldap:");
  $publisher_uri->host('ldap.internal.sanger.ac.uk');
  $publisher_uri->dn('ou=people,dc=sanger,dc=ac,dc=uk');
  $publisher_uri->attributes('title');
  $publisher_uri->scope('sub');
  $publisher_uri->filter("(uid=$uid)");

  return $publisher_uri;
}


=head2 get_publisher_name

  Arg [1]    : LDAP URI of publisher
  Example    : my $name = get_publisher_name($uri);
  Description: Returns the LDAP name of the user publishing data.
  Returntype : string
  Caller     : general

=cut

sub get_publisher_name {
  my ($uri) = @_;

  my $ldap = Net::LDAP->new($uri->host) or
    $log->logcroak("LDAP connection failed: ", $@);

  my $msg = $ldap->bind;
  $msg->code && $log->logcroak($msg->error);

  $msg = $ldap->search(base   => "ou=people,dc=sanger,dc=ac,dc=uk",
                       filter => $uri->filter);
  $msg->code && $log->logcroak($msg->error);

  my ($name) = ($msg->entries)[0]->get('cn');

  $ldap->unbind;
  $log->logcroak("Failed to find $uri in LDAP") unless $name;

  return $name;
}


=head2 publish_idat_files

  Arg [1]    : arrayref of IDAT file names
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : Infinium database handle
  Arg [6]    : SequenceScape Warehouse database handle
  Arg [7]    : DateTime object of publication
  Arg [8]    : Make iRODs groups as necessary if true

  Example    : my $n = publish_idat_files(\@files, $creator_uri,
                                          '/my/project', $publisher_uri,
                                          $ifdb, $ssdb, $now, $groups);
  Description: Publishes IDAT file pairs to iRODS with attendant metadata.
               Skips any files where consent is absent. Republishes any
               file that is already published, but whose checksum has
               changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_idat_files {
  my ($files, $creator_uri, $publish_dest, $publisher_uri,
      $ifdb, $ssdb, $time, $make_groups) = @_;

  my $paired = paired_idat_files($files, $log);
  my $pairs = scalar @$paired;
  my $total = $pairs * 2;
  my $published = 0;

  $log->debug("Publishing $pairs pairs of idat files");

  foreach my $pair (@$paired) {
    my ($red) = grep { m{Red}msxi } @$pair;
    my ($grn) = grep { m{Grn}msxi } @$pair;

    my ($basename, $dir, $suffix) = fileparse($red);

    $log->debug("Finding the sample for '$red' in the Infinium LIMS");
    my $if_sample = $ifdb->find_scanned_sample($basename);

    if ($if_sample) {
      eval {
        my @meta;
        push(@meta, make_warehouse_metadata($if_sample, $ssdb));
        push(@meta, make_infinium_metadata($if_sample));

        foreach my $file ($red, $grn) {
          publish_file($file, \@meta,  $creator_uri->as_string, $publish_dest,
                       $publisher_uri->as_string, $time, $make_groups, $log);
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
  Arg [8]    : Make iRODs groups as necessary if true

  Example    : my $n = publish_gtc_files(\@files, $creator_uri,
                                         '/my/project', $publisher_uri,
                                         $ifdb, $ssdb, $now, $groups);
  Description: Publishes GTC files to iRODS with attendant metadata.
               Skips any files where consent is absent. Republishes any
               file that is already published, but whose checksum has
               changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_gtc_files {
  my ($files, $creator_uri, $publish_dest, $publisher_uri,
      $ifdb, $ssdb, $time, $make_groups) = @_;

  my $total = scalar @$files;
  my $published = 0;

  $log->debug("Publishing $total GTC files");

  foreach my $file (@$files) {
    my ($basename, $dir, $suffix) = fileparse($file);

    $log->debug("Finding the sample for '$file' in the Infinium LIMS");
    my $if_sample = $ifdb->find_called_sample($basename);

    if ($if_sample) {
      eval {
        my @meta;

        push(@meta, make_warehouse_metadata($if_sample, $ssdb));
        push(@meta, make_infinium_metadata($if_sample));

        publish_file($file, \@meta, $creator_uri->as_string, $publish_dest,
                     $publisher_uri->as_string, $time, $make_groups, $log);
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


=head2 publish_analysis_directory

  Arg [1]    : directory containing the analysis results
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : genotyping pipeline database handle
  Arg [6]    : pipeline run name in pipeline database
  Arg [7]    : DateTime object of publication

  Example    : my $uuid =
                   publish_analysis_directory($dir, $creator_uri,
                                             '/my/project', $publisher_uri,
                                             $pipedb, 'run1', $now);
  Description: Publishes an analysis directory to an iRODS collection. Adds
               to the collection metadata describing the genotyping projects
               analysed and a new UUID for the analysis. It also locates the
               corresponding sample data in iRODS and cross-references it to
               the analysis by adding the UUID to the sample metadata.
  Returntype : a new UUID for the analysis
  Caller     : general

=cut

sub publish_analysis_directory {
  my ($dir, $creator_uri, $publish_dest, $publisher_uri, $pipedb, $run_name,
      $time) = @_;

  $publish_dest =~ s!/$!!;
  my $target = $publish_dest . '/' . basename($dir);

  if (list_collection($target)) {
    $log->logcroak("An iRODS collection already exists at '$target'. " .
                   "Please move or delete it before proceeding.");
  }

  $log->debug("Finding the project titles in the analysis database");

  my @project_titles;
  foreach my $dataset($pipedb->piperun->find({name => $run_name})->datasets) {
    push(@project_titles, $dataset->if_project);
  }

  unless (@project_titles) {
    $log->logcroak("The analysis database contained no data for run '$run_name'")
  }

  my $uuid;
  my $num_projects = 0;
  my $num_samples = 0;

  eval {
    my @meta;
    push(@meta, make_analysis_metadata(\@project_titles));
    push(@meta, make_creation_metadata($creator_uri, $time,  $publisher_uri));

    unless (list_collection($publish_dest)) {
      add_collection($publish_dest);
    }

    my $target = put_collection($dir, $publish_dest);
    $log->info("Created new collection $target");

    foreach my $elt (@meta) {
      my ($key, $value, $units) = @$elt;
      $log->debug("Adding key $key value $value to $target");
      add_collection_meta($target, $key, $value, $units);
    }

    my @uuid_meta = grep { $_->[0] =~ /uuid/ } @meta;
    $uuid = $uuid_meta[0]->[1];

    foreach my $title (@project_titles) {
      # Find the sample-level data for this analysis
      my @sample_data =  @{find_objects_by_meta($SAMPLE_DATA_ARCHIVE_PATTERN,
                                                'dcterms:title',
                                                $title)};
      if (@sample_data) {
        $log->info("Adding cross-reference metadata to " . scalar @sample_data .
                   " data objects in genotyping project '$title'");

        foreach my $target (@sample_data) {
          update_object_meta($target, \@uuid_meta);
          ++$num_samples;
        }

        ++$num_projects;
      }
      else {
        $log->warn("Found no sample data objects in iRODS for '$title'");
      }
    }
  };

  if ($@) {
    $log->error("Failed to publish: ", $@);
  }
  else {
    $log->info("Published '$dir' to '$publish_dest' and cross-referenced $num_samples data objects in $num_projects projects");
  }

  return $uuid;
}


sub publish_file {
  my ($file, $sample_meta, $creator_uri, $publish_dest, $publisher_uri,
      $time, $make_groups, $log) = @_;

  my $basename = fileparse($file);
  my $hash_path = hash_path($file);

  $publish_dest =~ s!/$!//!;
  my $dest_collection = $publish_dest . '/' . $hash_path;

  unless (list_collection($dest_collection)) {
    add_collection($dest_collection);
  }

  my $target = $dest_collection. '/' . $basename;

  my @meta = @$sample_meta;

  if (has_consent(@meta)) {
    if (list_object($target)) {
      if (checksum_object($target)) {
        $log->info("Skipping publication of $target because checksum is unchanged");
      }
      else {
        $log->info("Republishing $target because checksum is changed");
        $target = add_object($file, $target);
        push(@meta, make_modification_metadata($time));
      }
    }
    else {
      $log->info("Publishing $target");
      push(@meta, make_creation_metadata($creator_uri, $time, $publisher_uri));
      $target = add_object($file, $target);
    }

    push(@meta, make_file_metadata($file, '.idat', '.gtc'));

    update_object_meta($target, \@meta);

    my @groups = expected_irods_groups(@meta);
    foreach my $group (@groups) {
      $log->info("Giving group '$group' read access to $target");

      if ($make_groups) {
        set_group_access('read', find_or_make_group($group), $target);
      }
      else {
        if (group_exists($group)) {
          set_group_access('read', $group, $target);
        }
        else {
          $log->warn("Cannot give group '$group' access to $target because this group does not exist (in no-create groups mode)");
        }
      }
    }
  }
  else {
    $log->info("Skipping publication of $target because no consent was given");
  }

  return $target;
}


sub paired_idat_files {
  my ($files, $log) = @_;

  my %names;

  # Determine unique 
  foreach my $file (@$files) {
    my ($stem, $colour, $suffix) = $file =~ m{^(\S+)_(Red|Grn)(.idat)$}msxi;

    if ($stem && $colour && $suffix) {
      if (exists $names{$stem}) {
        push(@{$names{$stem}}, $file);
      }
      else {
        $names{$stem} = [$file];
      }
    }
    else {
      $log->warn("Found a non-idat file while sorting idat files: '$file'");
    }
  }

  my @paired;
  foreach my $stem (sort keys %names) {
    if (scalar @{$names{$stem}} == 2) {
      push(@paired, $names{$stem});
    }
    else {
      $log->warn("Ignoring an unpaired idat file with name stem '$stem'");
    }
  }

  return \@paired;
}


sub expected_irods_groups {
  my @meta = @_;

  my @ss_study_ids = metadata_for_key(\@meta, $WTSI::Genotyping::STUDY_ID_META_KEY);
  unless (@ss_study_ids) {
    $log->logconfess("Did not find any study information in metadata");
  }

  my @groups;
  foreach my $study_id (@ss_study_ids) {
    my $group_name = make_group_name($study_id);
    push(@groups, $group_name);
  }

  return @groups;
}


sub update_object_meta {
  my ($target, $meta) = @_;

  my %current_meta = get_object_meta($target);
  foreach my $elt (@$meta) {
    my ($key, $value, $units) = @$elt;

    if (meta_exists($key, $value, %current_meta)) {
      $log->debug("Skipping addition of key $key value $value to $target (exists)");
    }
    else {
      $log->debug("Adding key $key value $value to $target");
      add_object_meta($target, $key, $value, $units);
    }
  }
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
