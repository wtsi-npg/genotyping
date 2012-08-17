use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;
use File::Basename;

our $SAMPLE_NAME_META_KEY             = 'sample';
our $SAMPLE_ID_META_KEY               = 'sample_id';
our $SAMPLE_COMMON_NAME_META_KEY      = 'sample_common_name';
our $SAMPLE_ACCESSION_NUMBER_META_KEY = 'sample_accession_number';
our $SAMPLE_CONSENT_META_KEY          = 'sample_consent';

our $STUDY_ID_META_MEY                = 'study_id';
our $STUDY_TITLE_META_KEY             = 'study_title';


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
  my ($creation_time, $publisher) = @_;

  return (['dcterms:created' => $creation_time->iso8601],
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

  my $ss_sample = $ssdb->find_infinium_sample($if_barcode, $if_well);
  my @ss_studies = @{$ssdb->find_infinium_studies($if_barcode, $if_well)};

  my @meta = ([$SAMPLE_NAME_META_KEY    => $ss_sample->{name}],
              [$SAMPLE_ID_META_KEY      => $ss_sample->{internal_id}],
              [$SAMPLE_CONSENT_META_KEY => !$ss_sample->{consent_withdrawn}],
              ['dcterms:identifier'     => $ss_sample->{sanger_sample_id}]);

  if (defined $ss_sample->{accession_number}) {
    push(@meta, [$SAMPLE_ACCESSION_NUMBER_META_KEY =>
                 $ss_sample->{accession_number}]);
  }
  if (defined $ss_sample->{common_name}) {
    push(@meta, [$SAMPLE_COMMON_NAME_META_KEY => $ss_sample->{common_name}]);
  }

  foreach my $ss_study (@ss_studies) {
    push(@meta, [$STUDY_ID_META_MEY => $ss_study->{internal_id}]);

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

  return (['dcterms:identifier' => $if_sample->{sample}],
          [beadchip => $if_sample->{beadchip}]);
}


=head2 make_file_metadata

  Arg [1]    : string filename
  Arg [2]    : array of valid fie suffix strings
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

  my @meta = ([md5 => $md5],
              ['type' => $suffix]);

  return @meta;
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
