use utf8;

package WTSI::NPG::Metadata;

use strict;
use warnings;
use Carp;
use File::Basename;
use UUID;

use WTSI::NPG::iRODS qw(md5sum);

use base 'Exporter';
our @EXPORT_OK = qw($SAMPLE_NAME_META_KEY
                    $SAMPLE_ID_META_KEY
                    $SAMPLE_SUPPLIER_NAME_META_KEY
                    $SAMPLE_COMMON_NAME_META_KEY
                    $SAMPLE_ACCESSION_NUMBER_META_KEY
                    $SAMPLE_COHORT_META_KEY
                    $SAMPLE_CONTROL_META_KEY
                    $SAMPLE_CONSENT_META_KEY
                    $STUDY_ID_META_KEY
                    $STUDY_TITLE_META_KEY

                    make_creation_metadata
                    make_modification_metadata
                    make_file_metadata
                    make_sample_metadata

                    has_consent);

our $SAMPLE_NAME_META_KEY             = 'sample';
our $SAMPLE_ID_META_KEY               = 'sample_id';
our $SAMPLE_SUPPLIER_NAME_META_KEY    = 'sample_supplier_name';
our $SAMPLE_COMMON_NAME_META_KEY      = 'sample_common_name';
our $SAMPLE_ACCESSION_NUMBER_META_KEY = 'sample_accession_number';
our $SAMPLE_COHORT_META_KEY           = 'sample_cohort';
our $SAMPLE_CONTROL_META_KEY          = 'sample_control';
our $SAMPLE_CONSENT_META_KEY          = 'sample_consent';

our $STUDY_ID_META_KEY                = 'study_id';
our $STUDY_TITLE_META_KEY             = 'study_title';

our $log = Log::Log4perl->get_logger('npg.irods.publish');

=head2 make_creation_metadata

  Arg [1]    : DateTime creation time
  Arg [2]    : string publisher (LDAP URI of publisher)
  Example    : my @meta = make_creation_metadata($time, $publisher)
  Description: Return a list of metadata key/value pairs describing the
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
  Description: Return a list of metadata key/value pairs describing the
               creation of an item.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_modification_metadata {
  my ($modification_time) = @_;

  return (['dcterms:modified' => $modification_time]);
}

=head2 make_sample_metadata

  Arg [1]    : sample hashref from WTSI::NPG::Database::Warehouse
  Arg [2]    : WTSI::NPG::Database::Warehouse DB handle
  Example    : my @meta = make_sample_metadata($sample, $db)
  Description: Return a list of metadata key/value pairs describing the
               sample in the SequenceScape warehouse.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_sample_metadata {
  my ($ss_sample, $ssdb) = @_;

  my $internal_id = $ss_sample->{internal_id};

  my @meta = ([$SAMPLE_ID_META_KEY => $internal_id]);

  # These defensive checks are here because the SS data are sometimes missing
  my $message_template =
    "The %s value for sample with internal_id '$internal_id' " .
      "is missing from the Sequencescape Warehouse";

  if (defined $ss_sample->{name}) {
    push(@meta, [$SAMPLE_NAME_META_KEY => $ss_sample->{name}]);
  }
  else {
    $log->logcluck(sprintf($message_template, 'name'));
  }

  if (defined $ss_sample->{consent_withdrawn}) {
    push(@meta, [$SAMPLE_CONSENT_META_KEY => !$ss_sample->{consent_withdrawn}]);
  }
  else {
    $log->logcluck(sprintf($message_template, 'consent_withdrawn'));
  }

  if (defined $ss_sample->{sanger_sample_id}) {
    push(@meta, ['dcterms:identifier' => $ss_sample->{sanger_sample_id}]);
  }
  else {
    $log->logcluck(sprintf($message_template, 'sanger_sample_id'));
  }

  if (defined $ss_sample->{study_id}) {
     push(@meta, [$STUDY_ID_META_KEY => $ss_sample->{study_id}]);
  }
  else {
    $log->logcluck(sprintf($message_template, 'study_id'));
  }

  if (defined $ss_sample->{study_title}) {
    push(@meta, [$STUDY_TITLE_META_KEY => $ss_study->{study_title}]);
  }
  if (defined $ss_sample->{supplier_name}) {
    push(@meta, [$SAMPLE_SUPPLIER_NAME_META_KEY => $ss_sample->{supplier_name}]);
  }
  if (defined $ss_sample->{accession_number}) {
    push(@meta, [$SAMPLE_ACCESSION_NUMBER_META_KEY => $ss_sample->{accession_number}]);
  }
  if (defined $ss_sample->{cohort}) {
    push(@meta, [$SAMPLE_COHORT_META_KEY => $ss_sample->{cohort}]);
  }
  if (defined $ss_sample->{control}) {
    push(@meta, [$SAMPLE_CONTROL_META_KEY => $ss_sample->{control}]);
  }
  if (defined $ss_sample->{common_name}) {
    push(@meta, [$SAMPLE_COMMON_NAME_META_KEY => $ss_sample->{common_name}]);
  }

  return @meta;
}

=head2 make_file_metadata

  Arg [1]    : string filename
  Arg [2]    : array of valid file suffix strings
  Example    : my @meta = make_file_metadata($sample)
  Description: Return a list of metadata key/value pairs describing a file,
               including the file 'type' (suffix) and MD5 checksum.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_file_metadata {
  my ($file, @suffixes) = @_;

  my ($basename, $dir, $suffix) = fileparse($file, @suffixes);

  my $md5 = md5sum($file);
  $suffix =~ s{^\.?}{}msxi;

  my @meta = ([md5    => $md5],
              ['type' => $suffix]);

  return @meta;
}

=head2 has_consent

  Arg [1]    : metadata array
  Example    : My $consent = has_consent(@meta);
  Description: Return true if the sample metadata contains an indication that
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
