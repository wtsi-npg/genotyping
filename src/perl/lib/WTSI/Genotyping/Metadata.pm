use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;
use File::Basename;
use UUID;

use WTSI::Genotyping::iRODS qw(md5sum);

use vars qw($SAMPLE_NAME_META_KEY
            $SAMPLE_ID_META_KEY
            $SAMPLE_SUPPLIER_NAME_META_KEY
            $SAMPLE_COMMON_NAME_META_KEY
            $SAMPLE_ACCESSION_NUMBER_META_KEY
            $SAMPLE_CONSENT_META_KEY
            $STUDY_ID_META_KEY
            $STUDY_TITLE_META_KEY
            $GENOTYPING_PROJECT_TITLE_META_KEY
            $GENOTYPING_ANALYSIS_UUID_META_KEY);

$SAMPLE_NAME_META_KEY             = 'sample';
$SAMPLE_ID_META_KEY               = 'sample_id';
$SAMPLE_SUPPLIER_NAME_META_KEY    = 'sample_supplier_name';
$SAMPLE_COMMON_NAME_META_KEY      = 'sample_common_name';
$SAMPLE_ACCESSION_NUMBER_META_KEY = 'sample_accession_number';
$SAMPLE_CONSENT_META_KEY          = 'sample_consent';

$STUDY_ID_META_KEY                = 'study_id';
$STUDY_TITLE_META_KEY             = 'study_title';

$GENOTYPING_PROJECT_TITLE_META_KEY = 'dcterms:title';
$GENOTYPING_ANALYSIS_UUID_META_KEY = 'analysis_uuid';


our $log = Log::Log4perl->get_logger('npg.irods.publish');

=head2 make_creation_metadata

  Arg [1]    : DateTime creation time
  Arg [2]    : string publisher (LDAP URI of publisher)
  Example    : my @meta = make_creation_metadata($time, $publisher)
  Description: Returns a list of metadata key/value pairs describing the
               creation of an item.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_creation_metadata {
  my ($creator, $creation_time, $publisher) = @_;

  return (['dcterms:creator'   => $creator],
          ['dcterms:created'   => $creation_time->iso8601],
          ['dcterms:publisher' => $publisher]);
}


=head2 make_modification_metadata

  Arg [1]    : DateTime modification time
  Example    : my @meta = make_modification_metadata($time)
  Description: Returns a list of metadata key/value pairs describing the
               creation of an item.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_modification_metadata {
  my ($modification_time) = @_;

  return (['dcterms:modified' => $modification_time]);
}


=head2 make_warehouse_metadata

  Arg [1]    : sample hashref from WTSI::Genotyping::Database::Infinium
  Arg [2]    : WTSI::Genotyping::Database::Warehouse DB handle
  Example    : my @meta = make_warehouse_metadata($sample, $db)
  Description: Returns a list of metadata key/value pairs describing the
               sample in the SequenceScape warehouse.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_warehouse_metadata {
  my ($if_sample, $ssdb) = @_;

  my $if_barcode = $if_sample->{'plate'};
  my $if_well = $if_sample->{'well'};
  my $if_sample_name = $if_sample->{sample};
  my $if_chip = $if_sample->{beadchip};

  my $ss_sample = $ssdb->find_infinium_sample($if_barcode, $if_well);
  my @ss_studies = @{$ssdb->find_infinium_studies($if_barcode, $if_well)};

  my @meta = ([$SAMPLE_ID_META_KEY => $ss_sample->{internal_id}]);

  # These defensive checks are here because the SS data are sometimes missing
  if (defined $ss_sample->{name}) {
    push(@meta, [$SAMPLE_NAME_META_KEY => $ss_sample->{name}]);
  } else {
    $log->logcluck("The name value for $if_sample_name (chip $if_chip) ",
                   "is missing from the Sequencescape Warehouse");
  }

  if (defined $ss_sample->{consent_withdrawn}) {
    push(@meta, [$SAMPLE_CONSENT_META_KEY => !$ss_sample->{consent_withdrawn}]);
  } else {
    $log->logcluck("The consent_withdrawn value for $if_sample_name ",
                   "(chip $if_chip)is missing from the Sequencescape ",
                   "Warehouse");
  }

  if (defined $ss_sample->{sanger_sample_id}) {
    push(@meta, ['dcterms:identifier' => $ss_sample->{sanger_sample_id}]);
  } else {
    $log->logcluck("The sanger_sample_id value for $if_sample_name ",
                   "(chip $if_chip) is missing from the Sequencescape ",
                   "Warehouse");
  }

  if (defined $ss_sample->{supplier_name}) {
    push(@meta, [$SAMPLE_SUPPLIER_NAME_META_KEY => $ss_sample->{supplier_name}]);
  }

  if (defined $ss_sample->{accession_number}) {
    push(@meta, [$SAMPLE_ACCESSION_NUMBER_META_KEY => $ss_sample->{accession_number}]);
  }
  if (defined $ss_sample->{common_name}) {
    push(@meta, [$SAMPLE_COMMON_NAME_META_KEY => $ss_sample->{common_name}]);
  }

  foreach my $ss_study (@ss_studies) {
    push(@meta, [$STUDY_ID_META_KEY => $ss_study->{internal_id}]);

    if (defined $ss_study->{study_title}) {
      push(@meta, [$STUDY_TITLE_META_KEY => $ss_study->{study_title}]);
    }
  }

  return @meta;
}

=head2 make_infinium_metadata

  Arg [1]    : sample hashref from WTSI::Genotyping::Database::Infinium
  Example    : my @meta = make_infinium_metadata($sample)
  Description: Returns a list of metadata key/value pairs describing the
               sample in the Infinium LIMS. Includes the beadchip identifier.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_infinium_metadata {
  my ($if_sample) = @_;

  my $if_barcode = $if_sample->{'plate'};
  my $if_well = $if_sample->{'well'};
  my $if_sample_name = $if_sample->{sample};
  my $if_chip = $if_sample->{beadchip};

  return ([$GENOTYPING_PROJECT_TITLE_META_KEY => $if_sample->{project}],
          ['dcterms:identifier'               => $if_sample->{sample}],
          [beadchip                           => $if_sample->{beadchip}],
          [beadchip_section                   => $if_sample->{beadchip_section}],
          [beadchip_design                    => $if_sample->{beadchip_design}]);
}


=head2 make_file_metadata

  Arg [1]    : string filename
  Arg [2]    : array of valid file suffix strings
  Example    : my @meta = make_infinium_metadata($sample)
  Description: Returns a list of metadata key/value pairs describing a file,
               including the file 'type' (suffix) and MD5 checksum.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_file_metadata {
  my ($file, @suffixes) = @_;

  my ($basename, $dir, $suffix) = fileparse($file, @suffixes);

  my $md5 = md5sum($file);
  $suffix =~ s{^\.?}{}msx;

  my @meta = ([md5    => $md5],
              ['type' => $suffix]);

  return @meta;
}

=head2 make_analysis_metadata

  Arg [1]    : Arrayref of genotyping project titles
  Example    : my @meta = make_analysis_metadata($uuid, \@titles)
  Description: Returns a list of metadata key/value pairs describing an analysis
               including the genotyping project names involved.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_analysis_metadata {
  my ($genotyping_project_titles) = @_;

  my $uuid_bin;
  my $uuid_str;
  UUID::generate($uuid_bin);
  UUID::unparse($uuid_bin, $uuid_str);

  my @meta = ([$GENOTYPING_ANALYSIS_UUID_META_KEY => $uuid_str]);

  foreach my $title (@$genotyping_project_titles) {
    push(@meta, [$GENOTYPING_PROJECT_TITLE_META_KEY => $title]);
  }

  return @meta;
}

sub metadata_for_key {
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

=head2 has_consent

  Arg [1]    : metadata array
  Example    : My $consent = has_consent(@meta);
  Description: Returns true if the sample metadata contains an indication that
               consent has been given.
  Returntype : boolean
  Caller     : general

=cut

sub has_consent {
  my @meta = @_;

  my $consent = 0;
  my $found = 0;

  foreach my $pair (@meta) {
    my ($key, $value) = @$pair;
    if ($key eq $SAMPLE_CONSENT_META_KEY) {
      if ($found) {
        confess("Multiple consent keys are present in the metadata");
      }
      else {
        $found = 1;
      }

      if ($value) {
        $consent = $value;
      }
    }
  }

  return $consent;
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
