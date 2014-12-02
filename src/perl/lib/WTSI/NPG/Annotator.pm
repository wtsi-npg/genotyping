use utf8;

package WTSI::NPG::Annotator;

use Moose::Role;
use File::Basename;

use WTSI::NPG::Utilities qw(md5sum);

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Annotation';

our @DEFAULT_FILE_SUFFIXES = qw(.csv .gtc .idat .tif .tsv .txt .xls .xlsx .xml);

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

  Arg [1]    : sample hashref from WTSI::NPG::Database::Warehouse

  Example    : my @meta = $obj->make_sample_metadata($sample)
  Description: Return a list of metadata key/value pairs describing the
               sample in the SequenceScape warehouse.
  Returntype : array of arrayrefs

=cut

sub make_sample_metadata {
  my ($self, $ss_sample) = @_;

  my $internal_id = $ss_sample->{internal_id};

  my @meta = ([$self->sample_id_attr => $internal_id]);

  # These defensive checks are here because the SS data are sometimes missing
  my $message_template =
    "The %s value for sample with internal_id '$internal_id' " .
      "is missing from the Sequencescape Warehouse";

  if (defined $ss_sample->{name}) {
    push(@meta, [$self->sample_name_attr => $ss_sample->{name}]);
  }
  else {
    $self->logcluck(sprintf($message_template, 'name'));
  }

  if (defined $ss_sample->{consent_withdrawn}) {
    my $flag;
    if ($ss_sample->{consent_withdrawn}) {
      $flag = 0;
    }
    else {
      $flag = 1;
    }

    push(@meta, [$self->sample_consent_attr => $flag]);
  }
  else {
    $self->logcluck(sprintf($message_template, 'consent_withdrawn'));
  }

  if (defined $ss_sample->{sanger_sample_id}) {
    push(@meta, [$self->dcterms_identifier_attr => $ss_sample->{sanger_sample_id}]);
  }
  else {
    $self->logcluck(sprintf($message_template, 'sanger_sample_id'));
  }

  if (defined $ss_sample->{study_id}) {
     push(@meta, [$self->study_id_attr => $ss_sample->{study_id}]);
  }
  else {
    $self->logcluck(sprintf($message_template, 'study_id'));
  }

  if (defined $ss_sample->{study_title}) {
    push(@meta, [$self->study_title_attr => $ss_sample->{study_title}]);
  }
  if (defined $ss_sample->{supplier_name}) {
    push(@meta, [$self->sample_supplier_name_attr => $ss_sample->{supplier_name}]);
  }
  if (defined $ss_sample->{accession_number}) {
    push(@meta, [$self->sample_accession_number_attr => $ss_sample->{accession_number}]);
  }
  if (defined $ss_sample->{cohort}) {
    push(@meta, [$self->sample_cohort_attr => $ss_sample->{cohort}]);
  }
  if (defined $ss_sample->{control}) {
    push(@meta, [$self->sample_control_attr => $ss_sample->{control}]);
  }
  if (defined $ss_sample->{donor_id}) {
    push(@meta, [$self->sample_donor_id_attr => $ss_sample->{donor_id}]);
  }
  if (defined $ss_sample->{common_name}) {
    push(@meta, [$self->sample_common_name_attr => $ss_sample->{common_name}]);
  }

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
  $suffix =~ s{^\.?}{}msxi;

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

    push(@fingerprint, @tuple);
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

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
