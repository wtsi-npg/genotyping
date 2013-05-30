use utf8;

package WTSI::NPG::Publication;

use strict;
use warnings;
use Carp;
use File::Basename qw(basename fileparse);
use Net::LDAP;
use URI;

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

use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           make_creation_metadata
                           make_modification_metadata
                           make_file_metadata
                           make_sample_metadata
                           has_consent);

use base 'Exporter';
our @EXPORT_OK = qw(get_wtsi_uri
                    get_publisher_uri
                    get_publisher_name
                    pair_rg_channel_files
                    publish_file
                    update_object_meta
                    update_collection_meta
                    expected_irods_groups
                    grant_group_access);

our $log = Log::Log4perl->get_logger('npg.irods.publish');


=head2 get_wtsi_uri

  Example    : my $uri = get_wtsi_uri();
  Description: Return the URI of the WTSI.
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
  Description: Return the LDAP URI of the user publishing data.
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
  Description: Return the LDAP name of the user publishing data.
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

=head2 pair_rg_channel_files 

  Arg [1]    : arrayref of file names of a type, paired for Reg and Grn channels
  Example    : @paired = pair_rg_channel_files(['x_Red.idat', 'y_Red.idat',
                                                'y_Grn.idat', 'x_Grn.idat'], 'idat')
  Description: Return file names such that Red and Grn channels are paired. Pairing
               is determined by the file name and channel token (Red/Grn). Files
               not of the expected type are ignored.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub pair_rg_channel_files {
  my ($files, $type) = @_;

  my %names;

  # Determine unique
  foreach my $file (@$files) {
    my ($stem, $colour, $suffix) = $file =~ m{^(.+)_(Red|Grn)(.$type)$}msxi;

    if ($stem && $colour && $suffix) {
      if (exists $names{$stem}) {
        push(@{$names{$stem}}, $file);
      }
      else {
        $names{$stem} = [$file];
      }
    }
    else {
      $log->warn("Found a non-$type file while sorting $type files: '$file'");
    }
  }

  my @paired;
  foreach my $stem (sort keys %names) {
    if (scalar @{$names{$stem}} == 2) {
      push(@paired, $names{$stem});
    }
    else {
      $log->warn("Ignoring an unpaired $type file with name stem '$stem': " .
                 @{$names{$stem}}[0]);
    }
  }

  return @paired;
}

=head2 publish_file

  Arg [1]    : file name
  Arg [2]    : Sample metadata
  Arg [3]    : URI object of creator
  Arg [4]    : Publication path in iRODS
  Arg [5]    : URI object of publisher (typically an LDAP URI)
  Arg [6]    : DateTime object of publication
  Arg [7]    : Make iRODs groups as necessary if true
  Arg [8]    : Log4perl logger

  Example    : my $data_obj = publish_file($file, \@metadata, $creator_uri,
                                          '/my/file', $publisher_uri,
                                          $now, $groups);
  Description: Publish a file to iRODS with attendant metadata.
               Skip any file where consent is absent. Republish any
               file that is already published, but whose checksum has
               changed.
  Returntype : path to new iRODS data object
  Caller     : general

=cut

sub publish_file {
  my ($file, $sample_meta, $creator_uri, $publish_dest, $publisher_uri,
      $time, $make_groups, $log) = @_;

  my $basename = fileparse($file);
  # Make a path based on the file's MD5 to enable even distribution
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

    push(@meta, make_file_metadata($file, '.idat', '.gtc', '.xml', '.txt'));

    update_object_meta($target, \@meta);

    my @groups = expected_irods_groups(@meta);
    grant_group_access($target, 'read', $make_groups, @groups);
  }
  else {
    $log->info("Skipping publication of $target because no consent was given");
  }

  return $target;
}


=head2 expected_irods_groups

  Arg [1]    : array of arrayrefs (metadata)
  Example    : @groups = expected_irods_groups(@meta)
  Description: Return an array of iRODS group names given metadata containing
               >=1 study_id under the key $STUDY_ID_META_KEY
  Returntype : array of string
  Caller     : general

=cut

sub expected_irods_groups {
  my @meta = @_;

  my @ss_study_ids = _metadata_for_key(\@meta, $STUDY_ID_META_KEY);
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

=head2 update_object_meta

  Arg [1]    : iRODS data object
  Arg [2]    : array of arrayrefs (metadata)
  Example    : update_object_meta('/my/object', [[$key => $value]])
  Description: Update the iRODS metadata of an object by adding keys and values
               given in the second argument. No iRODS metadata are removed.
  Returntype : void
  Caller     : general

=cut

sub update_object_meta {
  my ($target, $meta) = @_;

  my $unique_meta = _remove_meta_duplicates($meta);
  my %current_meta = get_object_meta($target);

  foreach my $elt (@$unique_meta) {
    my ($key, $value, $units) = @$elt;

    if (meta_exists($key, $value, %current_meta)) {
      $log->debug("Skipping addition of key '$key' value '$value' to '$target' (exists)");
    }
    else {
      $log->debug("Adding key '$key' value '$value' to '$target'");
      add_object_meta($target, $key, $value, $units);
    }
  }
}

=head2 update_collection_meta

  Arg [1]    : iRODS collection
  Arg [2]    : array of arrayrefs (metadata)
  Example    : update_collection_meta('/my/collection', [[$key => $value]])
  Description: Update the iRODS metadata of a collection by adding keys and values
               given in the second argument. No iRODS metadata are removed.
  Returntype : void
  Caller     : general

=cut

sub update_collection_meta {
  my ($target, $meta) = @_;

  my $unique_meta = _remove_meta_duplicates($meta);
  my %current_meta = get_collection_meta($target);

  foreach my $elt (@$unique_meta) {
    my ($key, $value, $units) = @$elt;

    if (meta_exists($key, $value, %current_meta)) {
      $log->debug("Skipping addition of key '$key' value '$value' to '$target' (exists)");
    }
    else {
      $log->debug("Adding key '$key' value '$value' to '$target'");
      add_collection_meta($target, $key, $value, $units);
    }
  }
}

=head2 grant_group_access

  Arg [1]    : iRODS collection or data object
  Arg [2]    : iRODS access level string ('read', 'all' etc.)
  Arg [3]    : generate any required new groups
  Arg [4]    : array of group names
  Example    : grant_group_access('/my/object', 'read', 0, 'ss_1234', 'ss_1235')
  Description: Set iRODS group access on the spefied entity. If the 3rd argument
               is true, groups that do not exist will be created.
  Returntype : void
  Caller     : general

=cut

sub grant_group_access {
  my ($target, $access, $make_groups, @groups) = @_;

  foreach my $group (@groups) {
    $log->info("Giving group '$group' '$access' access to $target");

    if ($make_groups) {
      set_group_access($access, find_or_make_group($group), $target);
    }
    else {
      if (group_exists($group)) {
        set_group_access($access, $group, $target);
      }
      else {
        $log->warn("Cannot give group '$group' '$access' access to $target because this group does not exist (in no-create groups mode)");
      }
    }
  }
}

sub _metadata_for_key {
  my ($meta, $key) = @_;
  unless (defined $key) {
    $log->logconfess("Cannot find metadata for an undefined key");
  }

  my @values;

  foreach my $pair (@$meta) {
    my ($k, $value) = @$pair;

    if ($k eq $key) {
      push(@values, $value);
    }
  }

  return @values;
}

sub _remove_meta_duplicates {
  my ($meta) = @_;

  my %lookup_inc_units;
  my %lookup_exc_units;

  my @unique;
  foreach my $elt (@$meta) {
    my ($key, $value, $units) = @$elt;

    if (defined $units) {
      if (exists $lookup_inc_units{$key} &&
          exists $lookup_inc_units{$key}->{$value} &&
          exists $lookup_inc_units{$key}->{$value}->{$units}) {
        next;
      }
      else {
        $lookup_inc_units{$key}->{$value}->{$units} = 1;
        push(@unique, $elt);
      }
    }
    elsif (exists $lookup_exc_units{$key} &&
           exists $lookup_exc_units{$key}->{$value}) {
      next;
    }
    else {
      $lookup_exc_units{$key}->{$value} = 1;
      push(@unique, $elt);
    }
  }

  return \@unique;
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
