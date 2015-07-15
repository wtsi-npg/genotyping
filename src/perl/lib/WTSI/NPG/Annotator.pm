
package WTSI::NPG::Annotator;

use Data::Dump qw(dump);
use Moose::Role;
use File::Basename;

use WTSI::NPG::Utilities qw(md5sum);

our $VERSION = '';

our @DEFAULT_FILE_SUFFIXES = qw(.csv .gtc .idat .tif .tsv .txt .xls .xlsx .xml);

our $SEQUENCESCAPE_LIMS_ID = 'SQSCP';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Annotation';

=head2 make_creation_metadata

  Arg [1]    : DateTime creation time
  Arg [2]    : string publisher (LDAP URI of publisher)

  Example    : my @meta = $obj->make_creation_metadata($time, $publisher)
  Description: Return a list of metadata key/value pairs describing the
               creation of an item.
  Returntype : array of arrayrefs

=cut

sub make_creation_metadata {
  my ($self, $creator, $creation_time, $publisher) = @_;

  return ([$self->dcterms_creator_attr   => $creator->as_string],
          [$self->dcterms_created_attr   => $creation_time->iso8601],
          [$self->dcterms_publisher_attr => $publisher->as_string]);
}

=head2 make_modification_metadata

  Arg [1]    : DateTime modification time

  Example    : my @meta = $obj->make_modification_metadata($time)
  Description: Return a list of metadata key/value pairs describing the
               creation of an item.
  Returntype : array of arrayrefs

=cut

sub make_modification_metadata {
  my ($self, $modification_time) = @_;

  return ([$self->dcterms_modified_attr => $modification_time->iso8601]);
}

=head2 make_sample_metadata

  Arg [1]    : sample hashref from WTSI::NPG::Database::Warehouse or
               WTSI::NPG::Database::MLWarehouse

  Example    : my @meta = $obj->make_sample_metadata($record)
  Description: Return a list of metadata key/value pairs describing the
               sample in the SequenceScape warehouse.
  Returntype : array of arrayrefs

=cut

sub make_sample_metadata {
  my ($self, $record) = @_;

  # These defensive checks are here because the data are sometimes missing
  my $msg = "The '%s' value is missing from the warehouse record: %s";

  my @meta;

  if (defined $record->{consent_withdrawn}) {
    my $flag;
    if ($record->{consent_withdrawn}) {
      $flag = 0;
    }
    else {
      $flag = 1;
    }

    push @meta, [$self->sample_consent_attr => $flag];
  }
  else {
    $self->logcarp(sprintf($msg, 'consent_withdrawn', dump($record)));
  }

  # Ensure that these are added, or log an error.
  my $ensure = sub {
    my ($key, $meta_attr) = @_;
    if (defined $record->{$key}) {
      push @meta, [$meta_attr, $record->{$key}];
    }
    else {
      $self->logcarp(sprintf($msg, $key, dump($record)));
    }
  };

  # Maybe add these, only if present.
  my $maybe = sub {
    my ($key, $meta_attr) = @_;
    if (defined $record->{$key}) {
      push @meta, [$meta_attr, $record->{$key}];
    }
  };

  # The following is a shim to generate annotation from either the
  # Sequencescape warehouse or the Multi-LIMS warehouse. Only the ML
  # warehouse has an 'id_lims' key.
  if (defined $record->{id_lims}) {
    # This metadata obtained from from ML warehouse

    if ($record->{id_lims} eq $SEQUENCESCAPE_LIMS_ID) {
      # Sample processed by Sequencescape; sanger_sample_id must be
      # present
      $ensure->('sanger_sample_id', $self->dcterms_identifier_attr);
    }
    else {
      # Sample processed elsewhere; sanger_sample_id may not be
      # present
      $maybe->('sanger_sample_id', $self->dcterms_identifier_attr);
    }

    # Sample ID comes from 'id_sample_lims' column
    $ensure->('id_sample_lims', $self->sample_id_attr);
  }
  else {
    # This metadata obtained from Sequencescape warehouse.

    # Sample processed by Sequencescape; sanger_sample_id must be
    # present
    $ensure->('sanger_sample_id', $self->dcterms_identifier_attr);

    # Sample ID comes from 'internal_id' column
    $ensure->('internal_id', $self->sample_id_attr);
  }

  $ensure->('name',             $self->sample_name_attr);
  $ensure->('study_id',         $self->study_id_attr);

  $maybe->('study_title',      $self->study_title_attr);
  $maybe->('supplier_name',    $self->sample_supplier_name_attr);
  $maybe->('accession_number', $self->sample_accession_number_attr);
  $maybe->('cohort',           $self->sample_cohort_attr);
  $maybe->('control',          $self->sample_control_attr);
  $maybe->('donor_id',         $self->sample_donor_id_attr);
  $maybe->('common_name',      $self->sample_common_name_attr);

  return @meta;
}

=head2 make_type_metadata

  Arg [1]    : string filename
  Arg [2]    : array of valid file suffix strings

  Example    : my @meta = $obj->make_type_metadata($sample, '.txt', '.csv')
  Description: Return a list of metadata key/value pairs describing
               the file 'type' (suffix).
  Returntype : array of arrayrefs

=cut

sub make_type_metadata {
  my ($self, $file, @suffixes) = @_;

  unless (@suffixes) {
    @suffixes = @DEFAULT_FILE_SUFFIXES;
  }

  my ($basename, $dir, $suffix) = fileparse($file, @suffixes);
  $suffix =~ s{^[.]?}{}msxi;

  my @meta;
  if ($suffix) {
    push @meta, [$self->file_type_attr => $suffix];
  }

  return @meta;
}

=head2 make_md5_metadata

  Arg [1]    : string filename

  Example    : my @meta = $obj->make_md5_metadata($sample)
  Description: Return a list of metadata key/value pairs describing the
               file MD5 checksum.
  Returntype : array of arrayrefs

=cut

sub make_md5_metadata {
  my ($self, $file) = @_;

  my $md5 = md5sum($file);
  unless ($md5) {
    $self->logconfess("Failed to make MD5 for '$file'");
  }

  return ([$self->file_md5_attr => $md5]);
}

=head2 make_ticket_metadata

  Arg [1]    : string filename

  Example    : my @meta = $obj->make_ticket_metadata($ticket_number)
  Description: Return a list of metadata key/value pairs describing a
               ticket relating to the file
  Returntype : array of arrayrefs

=cut

sub make_ticket_metadata {
  my ($self, $ticket_number) = @_;

  return ([$self->ticket_attr => $ticket_number]);
}

sub make_fingerprint {
  my ($self, $keys, $meta) = @_;

  my @fingerprint;
  foreach my $key (@$keys) {
    my @tuple = grep { $_->[0] eq $key } @$meta;
    unless (@tuple) {
      my $meta_str = join(', ', map { join ' => ', @$_ } @$meta);
      $self->logconfess("Failed to make fingerprint from [$meta_str]: ",
                        "missing '$key'");
    }

    push @fingerprint, @tuple;
  }

  return @fingerprint;
}

no Moose;

1;

__END__

=head1 NAME

Annotator - a role which provides information to enable consistent
annotation.

=head1 DESCRIPTION

Provides a place to store metadata about an entity.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
