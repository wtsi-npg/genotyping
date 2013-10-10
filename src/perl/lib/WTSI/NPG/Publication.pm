use utf8;

package WTSI::NPG::Publication;

use strict;
use warnings;
use Carp;
use File::Basename qw(basename fileparse);
use Net::LDAP;
use URI;

use WTSI::NPG::iRODS qw(
                        add_collection
                        add_collection_meta
                        add_object
                        add_object_meta
                        calculate_checksum
                        find_objects_by_meta
                        find_or_make_group
                        find_zone_name
                        get_collection_meta
                        get_object_meta
                        group_exists
                        hash_path
                        list_collection
                        list_object
                        make_group_name
                        md5sum
                        meta_exists
                        move_object
                        put_collection
                        remove_object_meta
                        replace_object
                        set_group_access
                        validate_checksum_metadata
);

use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           has_consent
                           make_creation_metadata
                           make_md5_metadata
                           make_type_metadata
                           make_modification_metadata
                           make_sample_metadata);

use base 'Exporter';
our @EXPORT_OK = qw(expected_irods_groups
                    get_publisher_name
                    get_publisher_uri
                    get_wtsi_uri
                    grant_group_access
                    grant_study_access
                    pair_rg_channel_files
                    publish_file
                    publish_file_simply
                    supersede_object_meta
                    update_collection_meta
                    update_object_meta);

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
                                                'y_Grn.idat', 'x_Grn.idat'],
                                               'idat')
  Description: Return file names such that Red and Grn channels are paired.
               Pairing is determined by the file name and channel token
               (Red/Grn). Files not of the expected type are ignored.
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

=head2 publish_file_simply

  Arg [1]    : file name
  Arg [2]    : Sample metadata
  Arg [3]    : URI object of creator
  Arg [4]    : Publication path in iRODS
  Arg [5]    : URI object of publisher (typically an LDAP URI)
  Arg [6]    : DateTime object of publication

  Example    : my $data_obj = publish_file_simply($file, \@metadata,
                                           $creator_uri, '/my/file',
                                           $publisher_uri, $now);
  Description: Publish a file to iRODS with attendant metadata. Republish any
               file that is already published, but whose checksum has
               changed. This method does not look for other instances of
               the same data that may already be in another location in iRODS.
               It uses absolute data object paths to determine identity.
  Returntype : path to new iRODS data object
  Caller     : general

=cut

sub publish_file_simply {
  my ($file, $sample_meta, $creator_uri, $publish_dest, $publisher_uri,
      $time) = @_;

  my $basename = fileparse($file);
  my $md5 = md5sum($file);
  $log->debug("Checksum of file '$file' to be published is '$md5'");

  my $dest_collection = $publish_dest;
  $dest_collection =~ s!/$!//!;
  unless (list_collection($dest_collection)) {
    add_collection($dest_collection);
  }

  my $target = $dest_collection. '/' . $basename;
  my $zone = find_zone_name($target);

  my @meta = @$sample_meta;
  my $meta_str = join(', ', map { join ' => ', @$_ } @meta);

  if (list_object($target)) {
    my %existing_meta = get_object_meta($target);

    my $target_md5 = calculate_checksum($target);
    $log->debug("Checksum of existing target '$target' is '$target_md5'");
    if ($md5 eq $target_md5) {
      $log->info("Skipping publication of '$target' because the checksum ",
                 "is unchanged");
    }
    else {
      $log->info("Republishing '$target' in situ because the checksum ",
                 "is changed");
      $target = replace_object($file, $target);
      purge_object_meta($target, 'md5', \%existing_meta);
      push(@meta, make_md5_metadata($file));
      push(@meta, make_modification_metadata($time));
    }
  }
  else {
    $log->info("Publishing new object '$target'");
    $target = add_object($file, $target);
    push(@meta, make_type_metadata($file));
    push(@meta, make_md5_metadata($file));
    push(@meta, make_creation_metadata($creator_uri, $time, $publisher_uri));
  }

  update_object_meta($target, \@meta);

  return $target;
}

=head2 publish_file

  Arg [1]    : file name
  Arg [2]    : Sample metadata
  Arg [3]    : URI object of creator
  Arg [4]    : Publication path in iRODS
  Arg [5]    : URI object of publisher (typically an LDAP URI)
  Arg [6]    : DateTime object of publication

  Example    : my $data_obj = publish_file($file, \@metadata, $creator_uri,
                                          '/my/file', $publisher_uri, $now);
  Description: Publish a file to iRODS with attendant metadata. Republish any
               file that is already published, but whose checksum has
               changed. This method distributes files into paths based on
               the file checksum. It attempts to identify, using metadata,
               any existing files in iRODS that represent the same
               experimental result, possibly having different checksums.
               It uses data object basename and metadata to define identity.
  Returntype : path to new iRODS data object
  Caller     : general

=cut

sub publish_file {
  my ($file, $sample_meta, $creator_uri, $publish_dest, $publisher_uri,
      $time) = @_;

  my $basename = fileparse($file);

  # Make a path based on the file's MD5 to enable even distribution
  my $md5 = md5sum($file);
  my $hash_path = hash_path($file, $md5);
  $log->debug("Checksum of file '$file' to be published is '$md5'");

  $publish_dest =~ s!/$!//!;
  my $dest_collection = $publish_dest . '/' . $hash_path;
  unless (list_collection($dest_collection)) {
    add_collection($dest_collection);
  }

  my $target = $dest_collection. '/' . $basename;
  my $zone = find_zone_name($target);

  my @meta = @$sample_meta;
  my $meta_str = join(', ', map { join ' => ', @$_ } @meta);

  # Find existing data from same experiment, if any
  my @matching = find_objects_by_meta("/$zone", @meta);
  # Find existing copies of this file
  my @existing_objects = grep { fileparse($_) eq $basename } @matching;
  my $existing_object;
  my %existing_meta;

  if (scalar @existing_objects >1) {
    $log->logconfess("While publishing '$target' identified by ",
                     "{ $meta_str }, found >1 existing sample data: [",
                     join(', ', @existing_objects), "]");
  }
  elsif (@existing_objects) {
    $existing_object = $existing_objects[0];
    %existing_meta = get_object_meta($existing_object);
    my @existing_meta_str;
    foreach my $key (sort keys %existing_meta) {
      push(@existing_meta_str,
           "$key => [" . join(', ', @{$existing_meta{$key}}) . "]");
    }

    $log->logwarn("While publishing '$target' identified by ",
                  "{ $meta_str }, found existing sample data: ",
                  "'$existing_object' identified by { ",
                  join(', ', @existing_meta_str), " }");

    unless (exists $existing_meta{'md5'}) {
      $log->logwarn("Checksum metadata for existing sample data ",
                    "'$existing_object' is missing");
      $log->info("Adding missing checksum metadata for existing sample data ",
                 "'$existing_object'");
      update_object_meta($target, [make_md5_metadata($file)]);
    }

    unless (validate_checksum_metadata($existing_object)) {
      $log->error("Checksum metadata for existing sample data ",
                  "'$existing_object' is out of date");
    }
  }

  # Even with different MD5 values, the new and old data objects could
  # have the same path because we use only the first 3 bytes of the
  # hash string to define the collection path and the file basenames
  # are identical.

  if (list_object($target)) {
    my $target_md5 = calculate_checksum($target);
    $log->debug("Checksum of existing target '$target' is '$target_md5'");

    if ($md5 eq $target_md5) {
      $log->info("Skipping publication of '$target' because the checksum ",
                 "is unchanged");
    }
    else {
      $log->info("Republishing '$target' in situ because the checksum ",
                 "is changed");
      $target = replace_object($file, $target);
      purge_object_meta($target, 'md5', \%existing_meta);
      push(@meta, make_md5_metadata($file));
      push(@meta, make_modification_metadata($time));
    }
  }
  elsif ($existing_object) {
    my $existing_md5 = calculate_checksum($existing_object);
    $log->debug("Checksum of existing object '$existing_md5' ",
                "is '$existing_md5'");

    if ($md5 eq $existing_md5) {
      $log->info("Skipping publication of '$target' because existing object ",
                 "'$existing_object' exists with the same checksum");
    }
    else {
      $log->info("Moving '$existing_object' to '$target' and republishing",
                 "over it because the checksum is changed from ",
                 "'$existing_md5' to '$md5'");
      move_object($existing_object, $target); # Moves any metadata
      purge_object_meta($target, 'md5', \%existing_meta);
      push(@meta, make_md5_metadata($file));
      push(@meta, make_modification_metadata($time));
    }
  }
  else {
    $log->info("Publishing new object '$target'");
    $target = add_object($file, $target);
    push(@meta, make_creation_metadata($creator_uri, $time, $publisher_uri));
    push(@meta, make_type_metadata($file));
    push(@meta, make_md5_metadata($file));
  }

  update_object_meta($target, \@meta);

  return $target;
}

# sub redact_file {
#   my ($file, $sample_meta, $publish_dest, $publisher_uri, $ticket_number,
#       $time) = @_;

#   my $basename = fileparse($file);
#   my $md5 = md5sum($file);

#   my $target = list_object($publish_dest);
#   unless ($target) {
#     $log->logconfess("Failed to redact '$publish_dest': no such data object ",
#                      "exists");
#   }

#   my $meta_str = join(', ', map { join ' => ', @$_ } @$sample_meta);
#   if has_consent($sample_meta) {
#     $log->logconfess("Failed to redact '$publish_dest': it has consent");
#   }

#   my %existing_meta = get_object_meta($target);
#   if (exists $existing_meta{'md5'}) {
#     $log->info("Removing exisiting checksum metadata from '$target'");
#     purge_object_meta($target, 'md5', \%existing_meta);
#   }
#   else {
#     $log->logwarn("Checksum metadata for existing sample data ",
#                   "'$target' is missing");
#   }

#   my @meta;
#   push(@meta, [make_md5_metadata($file)]);
#   push(@meta, [make_modification_metadata($time)]);
#   push(@meta, [make_ticket_metadata($ticket_number)]);
#   push(@meta, @$sample_meta);

#   foreach my $meta (@$sample_meta) {
#     my ($key, $value, $unit) = @$meta;
#     my @purged = purge_object_meta($target, $key, \%existing_meta);
#     $log->debug("Purged values from of key '$key' from '$target': [",
#                 join(', ', @purged), "]");
#   }

#   update_object_meta($target, \@meta);

#   return $target;
# }


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

  unless (ref $meta eq 'ARRAY') {
    confess "meta argument must be an array reference\n";
  }

  my $unique_meta = _remove_meta_duplicates($meta);
  my %current_meta = get_object_meta($target);

  foreach my $elt (@$unique_meta) {
    my ($key, $value, $units) = @$elt;

    if (meta_exists($key, $value, %current_meta)) {
      $log->debug("Skipping addition of key '$key' ",
                  "value '$value' to '$target' (exists)");
    }
    else {
      add_object_meta($target, $key, $value, $units);
    }
  }
}


# sub supersede_object_meta {
#   my ($target, $meta) = @_;

#   my $unique_meta = _remove_meta_duplicates($meta);
#   my %current_meta = get_object_meta($target);

#   $log->debug("Superseding metadata on '$target'");

#   foreach my $elt (@$unique_meta) {
#     my ($key, $value, $units) = @$elt;

#     my $history_key = $key . '.history'

#     my @purged = purge_object_meta($target, $key, $unique_meta);
#     my @history;

#     if (exists $current_meta{$history_key}) {
#       @history = purge_object_meta($target, $history_key, $unique_meta);
#     }



#     my @new_history;
#     push(@new_history, @history, @purged);
#     add_object_meta($target, $history_key, join());

#     add_object_meta($target, $key, $value, $units);
#   }
# }

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
      $log->debug("Skipping addition of key '$key' ",
                  "value '$value' to '$target' (exists)");
    }
    else {
      $log->debug("Adding key '$key' value '$value' to '$target'");
      add_collection_meta($target, $key, $value, $units);
    }
  }
}

=head2 purge_object_meta

  Arg [1]    : iRODS data object name
  Arg [2]    : key
  Example    : purge_object_meta('/my/path/lorem.txt', 'id')
  Description: Remove all metadata for a particular key from a data object.
               Return an array of the removed values.
  Returntype : array
  Caller     : general

=cut

sub purge_object_meta {
  my ($object, $key, $meta) = @_;

  defined $object or $log->logconfess('A defined object argument is required');
  defined $key or $log->logconfess('A defined key argument is required');

  $object eq '' and $log->logconfess('A non-empty object argument is required');
  $key eq '' and $log->logconfess('A non-empty key argument is required');

  my @purged;
  if (exists $meta->{$key}) {
    my @values = @{$meta->{$key}};
    foreach my $value (@values) {
      remove_object_meta($object, $key, $value);
      push(@purged, $value);
    }
  }
  else {
    $log->logconfess("Metadata under key '$key' does not exist for $object");
  }

  return @purged;
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
    $log->logwarn("Did not find any study information in metadata");
  }

  my @groups;
  foreach my $study_id (@ss_study_ids) {
    my $group_name = make_group_name($study_id);
    push(@groups, $group_name);
  }

  return @groups;
}

=head2 grant_group_access

  Arg [1]    : iRODS collection or data object
  Arg [2]    : iRODS access level string ('read', 'all' etc.)
  Arg [3]    : array of group names
  Example    : grant_group_access('/my/object', 'read', 0, 'ss_1234', 'ss_1235')
  Description: Set iRODS group access on the spefied entity. If the 3rd argument
               is true, groups that do not exist will be created.
  Returntype : void
  Caller     : general

=cut

sub grant_group_access {
  my ($target, $access, @groups) = @_;

  foreach my $group (@groups) {
    $log->info("Giving group '$group' '$access' access to $target");

    set_group_access($access, $group, $target);
  }
}

sub grant_study_access {
  my ($target, $level, $meta) = @_;

  my $zone = find_zone_name($target);
  my @zoned_groups = map { "$_#$zone" } expected_irods_groups(@$meta);

  grant_group_access($target, $level, @zoned_groups);
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
