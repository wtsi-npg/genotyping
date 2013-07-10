use utf8;

package WTSI::NPG::Expression::Publication;

use strict;
use warnings;
use Carp;
use Cwd qw(abs_path);
use Digest::MD5 qw(md5_hex);
use File::Basename qw(basename fileparse);
use List::MoreUtils qw(uniq);
use Net::LDAP;
use URI;

use Data::Dumper;

use WTSI::NPG::Expression::Metadata qw(make_infinium_metadata
                                       make_analysis_metadata);

use WTSI::NPG::iRODS qw(hash_path
                        add_collection
                        list_collection
                        put_collection);

use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           make_creation_metadata
                           make_modification_metadata
                           make_file_metadata
                           make_sample_metadata);

use WTSI::NPG::Publication qw(publish_file
                              update_object_meta
                              update_collection_meta
                              expected_irods_groups
                              grant_group_access);

use base 'Exporter';
our @EXPORT_OK = qw(publish_expression_analysis);

our $log = Log::Log4perl->get_logger('npg.irods.publish');

=head2 publish_expression_analysis

  Arg [1]    : Directory containing the Genome Studio export file name
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : arrayref of sample specs from a chip loading manifest
  Arg [6]    : SequenceScape Warehouse database handle
  Arg [7]    : DateTime object of publication
  Arg [8]    : Make iRODs groups as necessary if true

  Example    : my $n = publish_expression_analysis($file, $files,
                                                   $creator_uri,
                                                   '/my/project',
                                                   $publisher_uri,
                                                   \@samples,
                                                   $ssdb, $now, $groups);
  Description: Publish a Genome Studio export, IDAT and XML file pairs to
               iRODS with attendant metadata. Skip any files where consent
               is absent. Republish any file that is already published,
               but whose checksum has changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_expression_analysis{
  my ($dir, $creator_uri,  $publish_analysis_dest, $publish_samples_dest,
      $publisher_uri, $samples, $ssdb, $time, $make_groups) = @_;

  my @beadchips = uniq(map { $_->{beadchip} } @$samples);
  my @sections = map { $_->{beadchip_section} } @$samples;

  # Make a hash path from the absolute path to the analysis
  my $hash_path = hash_path(undef, md5_hex(abs_path($dir)));
  $publish_analysis_dest =~ s!/$!!;
  my $analysis_target = join('/', $publish_analysis_dest, $hash_path);
  my $leaf_collection = join('/', $analysis_target, basename($dir));

  if (list_collection($leaf_collection)) {
    $log->logcroak("An iRODS collection already exists at '$leaf_collection'. " .
                   "Please move or delete it before proceeding.");
  }

  my $analysis_coll;
  my $uuid;
  my $num_samples = 0;

  eval {
  # Analysis directory
    my @analysis_meta;
    push(@analysis_meta, make_analysis_metadata());
    push(@analysis_meta, make_creation_metadata($creator_uri, $time,
                                                $publisher_uri));

    unless (list_collection($analysis_target)) {
      add_collection($analysis_target);
    }

    $analysis_coll = put_collection($dir, $analysis_target);
    $log->info("Created new collection $analysis_coll");

    my @uuid_meta = grep { $_->[0] =~ /uuid/ } @analysis_meta;
    $uuid = $uuid_meta[0]->[1];

    # Corresponding samples
    my $total = scalar @$samples * 2;
    my $published = 0;

    my %studies_seen;

    foreach my $sample (@$samples) {
      my $ss_sample = $ssdb->find_infinium_gex_sample($sample->{sanger_sample_id});
      my $study_id = $ss_sample->{study_id};

      unless (exists $studies_seen{$study_id}) {
        push(@analysis_meta, [$STUDY_ID_META_KEY => $study_id]);
        $studies_seen{$study_id}++;
      }

      my @meta;
      push(@meta, make_infinium_metadata($sample));
      push(@meta, make_sample_metadata($ss_sample, $ssdb));
      push(@meta, @uuid_meta);

      publish_file($sample->{idat_path}, \@meta, $creator_uri->as_string,
                   $publish_samples_dest, $publisher_uri->as_string, $time,
                   $make_groups, $log);
      publish_file($sample->{xml_path}, \@meta, $creator_uri->as_string,
                   $publish_samples_dest, $publisher_uri->as_string, $time,
                   $make_groups, $log);

      $num_samples++;
    }

    update_collection_meta($analysis_coll, \@analysis_meta);

    my @groups = expected_irods_groups(@analysis_meta);
    grant_group_access($analysis_coll, '-r read', $make_groups, @groups);
  };

  if ($@) {
    $log->error("Failed to publish: ", $@);
  }
  else {
    $log->info("Published '$dir' to '$analysis_coll' and cross-referenced $num_samples data objects");
  }

  return $uuid;
}

1;

__END__

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
