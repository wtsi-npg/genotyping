use utf8;

package WTSI::NPG::Expression::Publication;

use strict;
use warnings;
use Carp;
use Cwd qw(abs_path);
use Digest::MD5 qw(md5_hex);
use File::Basename qw(basename fileparse);
use List::MoreUtils qw(firstidx uniq);
use Net::LDAP;
use URI;

use Data::Dumper;

use WTSI::NPG::Expression::Metadata qw(infinium_fingerprint
                                       make_infinium_metadata
                                       make_analysis_metadata);

use WTSI::NPG::iRODS qw(hash_path
                        add_collection
                        list_collection
                        put_collection);

use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           make_creation_metadata
                           make_modification_metadata
                           make_sample_metadata);

use WTSI::NPG::Publication qw(publish_file
                              update_object_meta
                              update_collection_meta
                              expected_irods_groups
                              grant_group_access);
use WTSI::NPG::Utilities qw(trim);

use base 'Exporter';
our @EXPORT_OK = qw(publish_expression_analysis
                    parse_beadchip_table_v1
                    parse_beadchip_table_v2);

our $log = Log::Log4perl->get_logger('npg.irods.publish');


=head2 publish_expression_analysis

  Arg [1]    : Directory containing the Genome Studio export file name
  Arg [2]    : URI object of creator
  Arg [3]    : string publication destination in iRODS
  Arg [4]    : URI object of publisher (typically an LDAP URI)
  Arg [5]    : arrayref of sample specs from a chip loading manifest
  Arg [6]    : SequenceScape Warehouse database handle
  Arg [7]    : DateTime object of publication
  Arg [8]    : Supplied analysis UUID to use (optional)

  Example    : my $n = publish_expression_analysis($file, $files,
                                                   $creator_uri,
                                                   '/my/project',
                                                   $publisher_uri,
                                                   \@samples,
                                                   $ssdb, $now);
  Description: Publish a Genome Studio export, IDAT and XML file pairs to
               iRODS with attendant metadata. Skip any files where consent
               is absent. Republish any file that is already published,
               but whose checksum has changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_expression_analysis{
  my ($dir, $creator_uri,  $publish_analysis_dest, $publish_samples_dest,
      $publisher_uri, $samples, $ssdb, $time, $uuid) = @_;

  my @beadchips = uniq(map { $_->{beadchip} } @$samples);
  my @sections = map { $_->{beadchip_section} } @$samples;

  # Make a hash path from the absolute path to the analysis
  my $hash_path = hash_path(undef, md5_hex(abs_path($dir)));
  $publish_analysis_dest =~ s!/$!!;
  my $analysis_target = join('/', $publish_analysis_dest, $hash_path);
  my $leaf_collection = join('/', $analysis_target, basename($dir));

  unless ($uuid) {
    if (list_collection($leaf_collection)) {
      $log->logcroak("An iRODS collection already exists at ",
                     "'$leaf_collection'. ",
                     "Please move or delete it before proceeding.");
    }
  }

  my $analysis_coll;
  my $analysis_uuid;
  my $num_samples = 0;

  eval {
    # Analysis directory
    my @analysis_meta;
    push(@analysis_meta, make_analysis_metadata($uuid));

    if (list_collection($leaf_collection)) {
      $log->info("Collection $leaf_collection exists; updating metadata only");
      $analysis_coll = $leaf_collection;
    }
    else {
      add_collection($analysis_target);
      push(@analysis_meta, make_creation_metadata($creator_uri, $time,
                                                  $publisher_uri));
      $analysis_coll = put_collection($dir, $analysis_target);
      $log->info("Created new collection $analysis_coll");
    }

    my @uuid_meta = grep { $_->[0] =~ /uuid/ } @analysis_meta;
    $analysis_uuid = $uuid_meta[0]->[1];

    # Corresponding samples
    my $total = scalar @$samples * 2;
    my $published = 0;

    my %studies_seen;

    foreach my $sample (@$samples) {
      my $barcode = $sample->{infinium_plate};
      my $map = $sample->{infinium_well};
      my $sanger_id = $sample->{sanger_sample_id};

      my @sample_meta;
      push(@sample_meta, make_infinium_metadata($sample));

      my $ss_sample;
      if (defined $barcode && defined $map) {
        $ss_sample = $ssdb->find_infinium_gex_sample($barcode, $map);
        my $expected_sanger_id = $ss_sample->{sanger_sample_id};
        unless ($sanger_id eq $expected_sanger_id) {
          $log->logcroak("Sample in plate '$barcode' well '$map' ",
                         "has an incorrect Sanger sample ID '$sanger_id' ",
                         "(expected '$expected_sanger_id'");
        }
      }
      else {
        $ss_sample = $ssdb->find_infinium_gex_sample_by_sanger_id($sanger_id);
        $log->warn("Plate tracking information is absent for sample ",
                   $sanger_id, "; using Sanger sample ID instead");
      }

      my $study_id = $ss_sample->{study_id};
      unless (exists $studies_seen{$study_id}) {
        push(@analysis_meta, [$STUDY_ID_META_KEY => $study_id]);
        $studies_seen{$study_id}++;
      }

      push(@sample_meta, make_sample_metadata($ss_sample, $ssdb));
      push(@sample_meta, @uuid_meta);

      my @fingerprint = infinium_fingerprint(@sample_meta);
      my $idat_object =
        publish_file($sample->{idat_path}, \@fingerprint,
                     $creator_uri->as_string,
                     $publish_samples_dest, $publisher_uri->as_string, $time);
      update_object_meta($idat_object, \@sample_meta);

      my $xml_object =
        publish_file($sample->{xml_path}, \@fingerprint,
                     $creator_uri->as_string,
                     $publish_samples_dest, $publisher_uri->as_string, $time);
      update_object_meta($xml_object, \@sample_meta);

      my @groups = expected_irods_groups(@sample_meta);
      grant_group_access($idat_object, 'read', @groups);
      grant_group_access($xml_object, 'read', @groups);

      $num_samples++;

      $log->info("Cross-referenced $num_samples/", scalar @$samples,
                 " samples");
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
    $log->info("Published '$dir' to '$analysis_coll' and cross-referenced ",
               "$num_samples data objects");
  }

  return $analysis_uuid;
}


# Expects a tab-delimited text file. Useful data start after line
# containing column headers. This line is identified by the parser by
# presence of the the string 'BEADCHIP'.
#
# Column header  Content
# 'SAMPLE ID'    Sanger sample ID
# 'BEADCHIP'     Infinium Beadchip number
# 'ARRAY'        Infinium Beadchip section
#
# Data lines follow the header, the zeroth column of which contain an
# arbitrary string. This string is the same on all data containing
# lines.
#
# Data rows are terminated by a line containing the string
# 'Kit Control' in the zeroth column. This line is ignored by the
# parser.
#
# Any whitespace-only lines are ignored.
sub parse_beadchip_table_v1 {
  my ($fh) = @_;
  binmode($fh, ':utf8');

  # For error reporting
  my $line_count = 0;

  # Leftmost column; used only to determine which rows have sample data
  my $sample_key_col = 0;
  my $sample_key;

  # Columns containing useful data
  my $sample_id_col;
  my $beadchip_col;
  my $section_col;

  # True if we are past the header and into a data block
  my $in_sample_block = 0;

  # Collected data
  my @samples;

  while (my $line = <$fh>) {
    ++$line_count;
    chomp($line);
    next if $line =~ m/^\s*$/;

    if ($in_sample_block) {
      my @sample_row = map { trim($_) } split("\t", $line);
      unless ($sample_row[$sample_key_col]) {
        $log->logcroak("Premature end of sample data at line $line_count\n");
      }

      if (!defined $sample_key) {
        $sample_key = $sample_row[$sample_key_col];
      }

      if ($sample_key eq $sample_row[$sample_key_col]) {
        push(@samples,
             {sanger_sample_id => validate_sample_id($sample_row[$sample_id_col],
                                                     $line_count),
              beadchip         => validate_beadchip($sample_row[$beadchip_col],
                                                    $line_count),
              beadchip_section => validate_section($sample_row[$section_col],
                                                   $line_count)});
      }
      elsif ($sample_row[$sample_key_col] eq 'Kit Control') {
        # This token is taken to mean the data block has ended
        last;
      }
      else {
        $log->logcroak("Premature end of sample data at line $line_count " .
                       "(missing 'Kit Control')\n");
      }
    }
    else {
      if ($line =~ m/BEADCHIP/) {
        $in_sample_block = 1;
        my @header = map { trim($_) } split("\t", $line);
        # Expected to be Sanger sample ID
        $sample_id_col = firstidx { /SAMPLE ID/ } @header;
        # Expected to be chip number
        $beadchip_col  = firstidx { /BEADCHIP/ } @header;
        # Expected to be chip section
        $section_col = firstidx { /ARRAY/ } @header;
      }
    }
  }

  my $channel = 'Grn';
  foreach my $sample (@samples) {
    my $basename = sprintf("%s_%s_%s",
                           $sample->{beadchip},
                           $sample->{beadchip_section},
                           $channel);

    $sample->{idat_file} = $basename . '.idat';
    $sample->{xml_file}  = $basename . '.xml' ;
  }

  return @samples;
}

# Expects a tab-delimited text file. Useful data start after line
# containing column headers. This line is identified by the parser by
# presence of the the string 'BEADCHIP'.
#
# Column header       Content
# ''                  <ignored>
# 'Supplier Plate ID' Sequencescape plate ID
# 'SAMPLE ID'         Sanger sample ID
# 'Suppier WELL ID'   Sequencescape well ID
# 'BEADCHIP'     Infinium Beadchip number
# 'ARRAY'        Infinium Beadchip section
#
# Column headers are case insensitive. Columns may be in any order.
#
# Data lines follow the header, the zeroth column of which contain an
# arbitrary string which is ignored (it is for lab internal use).
#
# Data rows are terminated by a line containing the string
# 'Kit Control' in the zeroth column. This line is ignored by the
# parser.
#
# Any whitespace-only lines are ignored.
sub parse_beadchip_table_v2 {
  my ($fh) = @_;
  binmode($fh, ':utf8');

  # For error reporting
  my $line_count = 0;

  # Columns containing useful data
  my $end_of_data_col = 0;
  my $sample_id_col;
  my $supplier_plate_id_col;
  my $supplier_well_id_col;
  my $beadchip_col;
  my $section_col;

  # True if we are past the header and into a data block
  my $in_sample_block = 0;

  # Collected data
  my @samples;

  while (my $line = <$fh>) {
    ++$line_count;
    chomp($line);
    next if $line =~ m/^\s*$/;

    if ($in_sample_block) {
      my @sample_row = map { trim($_) } split("\t", $line);

      if ($sample_row[$end_of_data_col] =~ /^Kit Control$/i) {
        last;
      }
      else {
        push(@samples,
             {sanger_sample_id => validate_sample_id($sample_row[$sample_id_col], $line_count),
              infinium_plate   => validate_plate_id($sample_row[$supplier_plate_id_col], $line_count),
              infinium_well    => validate_well_id($sample_row[$supplier_well_id_col], $line_count),
              beadchip         => validate_beadchip($sample_row[$beadchip_col], $line_count),
              beadchip_section => validate_section($sample_row[$section_col], $line_count)});
      }
    }
    else {
      if ($line =~ m/BEADCHIP/i) {
        $in_sample_block = 1;
        my @header = map { trim($_) } split("\t", $line);
        # Expected to be Sanger sample ID
        $sample_id_col = firstidx { /SAMPLE ID/i } @header;
        # Expected to be Sequencescape plate ID
        $supplier_plate_id_col = firstidx { /SUPPLIER PLATE ID/i } @header;
        # Expected to be Sequencescape welll map
        $supplier_well_id_col = firstidx { /SUPPLIER WELL ID/i } @header;
        # Expected to be chip number
        $beadchip_col  = firstidx { /BEADCHIP/i } @header;
        # Expected to be chip section
        $section_col = firstidx { /ARRAY/i } @header;
      }
    }
  }

  my $channel = 'Grn';
  foreach my $sample (@samples) {
    my $basename = sprintf("%s_%s_%s",
                           $sample->{beadchip},
                           $sample->{beadchip_section},
                           $channel);

    $sample->{idat_file} = $basename . '.idat';
    $sample->{xml_file}  = $basename . '.xml' ;
  }

  return @samples;
}

sub validate_sample_id {
  my ($sample_id, $line) = @_;

  unless (defined $sample_id) {
    $log->logcroak("Missing sample ID at line $line\n");
  }

  if ($sample_id !~ /^\S+$/) {
    $log->logcroak("Invalid sample ID '$sample_id' at line $line\n");
  }

  return $sample_id;
}

sub validate_plate_id {
  my ($plate_id, $line) = @_;

  unless (defined $plate_id) {
    $log->logcroak("Missing Supplier Plate ID at line $line\n");
  }

  if ($plate_id !~ /^\S+$/) {
    $log->logcroak("Invalid Supplier plate ID '$plate_id' at line $line\n");
  }

  return $plate_id;
}

sub validate_well_id {
  my ($well_id, $line) = @_;

  unless (defined $well_id) {
    $log->logcroak("Missing Supplier well ID at line $line\n");
  }

  my ($row, $column) = $well_id =~ /^([A-H])([1-9]+[0-2]?)$/;
  unless ($row && $column) {
    $log->logcroak("Invalid Supplier well ID '$well_id' at line $line\n");
  }
  unless ($column >= 1 && $column <= 12) {
    $log->logcroak("Invalid Supplier well ID '$well_id' at line $line\n");
  }

  return $well_id;
}

sub validate_beadchip {
  my ($chip, $line) = @_;

  unless (defined $chip) {
    $log->logcroak("Missing beadchip number at line $line\n");
  }

  if ($chip !~ /^\d{10}$/) {
    $log->logcroak("Invalid beadchip number '$chip' at line $line\n");
  }

  return $chip;
}

sub validate_section {
  my ($section, $line) = @_;

  unless (defined $section) {
    $log->logcroak("Missing beadchip section at line $line\n");
  }

  if ($section !~ /^[A-Z]$/) {
    $log->logcroak("Invalid beadchip section '$section' at line $line\n");
  }

  return $section;
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
