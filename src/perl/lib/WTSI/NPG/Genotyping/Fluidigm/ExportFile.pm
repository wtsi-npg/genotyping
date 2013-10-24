
package WTSI::NPG::Genotyping::Fluidigm::ExportFile;

use Moose;

use WTSI::NPG::Genotyping::Publication qw(parse_fluidigm_table);

use WTSI::NPG::Genotyping::Metadata qw($FLUIDIGM_PLATE_NAME_META_KEY
                                       $FLUIDIGM_PLATE_WELL_META_KEY);

our $HEADER_BARCODE_ROW = 0;
our $HEADER_BARCODE_COL = 2;

our $HEADER_CONF_THRESHOLD_ROW = 5;
our $HEADER_CONF_THRESHOLD_COL = 1;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Addressable';

has 'file_name' => (is  => 'ro', isa => 'Str', required => 1);

has 'header' => (is  => 'ro', isa => 'ArrayRef[Str]',
                 writer => '_write_header');

has 'column_names' => (is => 'ro', isa => 'ArrayRef[Str]',
                       writer => '_write_column_names');

has 'fluidigm_barcode' => (is => 'ro', isa => 'Str', required => 1,
                           builder => '_build_fluidigm_barcode',
                           lazy => 1);

has 'confidence_threshold' => (is => 'ro', isa => 'Str', required => 1,
                               builder => '_build_confidence_threshold',
                               lazy => 1);

sub BUILD {
  my ($self) = @_;

  -e $self->file_name or
    $self->logdie("Fluidigm export file '", $self->file_name,
                  "' does not exist");

  open my $in, '<:encoding(utf8)', $self->file_name
    or $self->logdie("Failed to open Fluidigm export file '",
                     $self->file_name, "': $!");

  my ($header, $column_names, $sample_data) = parse_fluidigm_table($in);
  close $in;

  $self->_write_header($header);
  $self->_write_column_names($column_names);
  $self->_write_content($sample_data);
}

sub _build_fluidigm_barcode {
  my ($self) = @_;

  my @header = @{$self->header};
  my @fields = split ',', $header[$HEADER_BARCODE_ROW];

  return $fields[$HEADER_BARCODE_COL];
}

sub _build_confidence_threshold {
  my ($self) = @_;

  my @header = @{$self->header};
  my @fields = split ',', $header[$HEADER_CONF_THRESHOLD_ROW];

  return $fields[$HEADER_CONF_THRESHOLD_COL];
}

=head2 sample_assays

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Example    : my $assays = $export->sample_assays('S01')
  Description: Return a copy of the assay results for this sample
  Returntype : ArrayRef
  Caller     : general

=cut

sub sample_assays {
  my ($self, $address) = @_;

  return [@{$self->lookup($address)}];
}

=head2 write_sample_assays

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Arg [2]    : File name
  Example    : $export->write_sample_assays('S01', $file)
  Description: Write a tab-delimited CSV file containing the assay results
               for one sample. Returns the number of records written.
  Returntype : Str
  Caller     : general

=cut

sub write_sample_assays {
  my ($self, $address, $file_name) = @_;

  defined $address or
    $self->logconfess("A defined address argument is required");
  defined $file_name or
    $self->logconfess("A defined file_name argument is required");

  my $records_written = 0;
  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});

  $csv->column_names($self->column_names);

  open my $out, '>:encoding(utf8)', $file_name
    or $self->logcroak("Failed to open Fluidigm CSV file '$file_name' ",
                       "for writing: $!");

  my @assays = @{$self->sample_assays($address)};
  foreach my $assay (@assays) {
    $csv->print($out, $assay)
      or $self->logcroak("Failed to write record [", join(", ", @$assay),
                         "] to '$file_name': ", $csv->error_diag);
    ++$records_written;
  }

  close $out;

  return $records_written;
}

=head2 fluidigm_metadata

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Example    : $export->fluidigm_metadata('S01')
  Description: Return the metadata for the sample assayed at the address.
  Returntype : ArrayRef
  Caller     : general

=cut

sub fluidigm_metadata {
  my ($self, $address) = @_;

  defined $address or
    $self->logconfess("A defined address argument is required");
  exists $self->content->{$address} or
    $self->logconfess("FluidigmExportFile '", $self->fluidigm_barcode,
                      "' has no sample address '$address'");

  return ([$FLUIDIGM_PLATE_NAME_META_KEY => $self->fluidigm_barcode],
          [$FLUIDIGM_PLATE_WELL_META_KEY => $address]);
}

=head2 fluidigm_fingerprint

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Example    : $export->fluidigm_metadata('S01')
  Description: Return the metadata fingerprint for the sample assayed at
               the address.
  Returntype : ArrayRef
  Caller     : general

=cut

sub fluidigm_fingerprint {
  my ($self, $address) = @_;

  defined $address or
    $self->logconfess("A defined address argument is required");

  return $self->fluidigm_metadata($address);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

FluidigmExportFile - A structured CSV data file exported by Fluidigm
instrument software.

=head1 SYNOPSIS

  my $export = WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
    ({file_name => 'results.csv'});

  print $export->fluidigm_barcode, "\n";
  print $export->confidence_threshold, "\n";
  print $export->size, "\n";

=head1 DESCRIPTION

A wrapper for the structured CSV data file exported by Fluidigm
instrument software. It performs an eager parse of the file and
provides methods to access and manipulate the data.

FluidigmExportFile consumes the WTSI::NPG::Addressable role.

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
