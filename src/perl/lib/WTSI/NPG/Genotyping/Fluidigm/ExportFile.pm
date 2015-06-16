
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::ExportFile;

use Moose;
use Text::CSV;

use WTSI::NPG::Utilities qw(trim);

our $VERSION = '';

our $HEADER_BARCODE_ROW = 0;
our $HEADER_BARCODE_COL = 2;

our $HEADER_CONF_THRESHOLD_ROW = 5;
our $HEADER_CONF_THRESHOLD_COL = 1;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Addressable',
  'WTSI::NPG::Genotyping::Annotation';

has 'file_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'header' =>
  (is     => 'ro',
   isa    => 'ArrayRef[Str]',
   writer => '_write_header');

has 'column_names' =>
  (is     => 'ro',
   isa    => 'ArrayRef[Str]',
   writer => '_write_column_names');

has 'fluidigm_barcode' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   lazy     => 1,
   builder  => '_build_fluidigm_barcode');

has 'confidence_threshold' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   lazy     => 1,
   builder  => '_build_confidence_threshold');

sub BUILD {
  my ($self) = @_;

  -e $self->file_name or
    $self->logdie("Fluidigm export file '", $self->file_name,
                  "' does not exist");

  open my $in, '<:encoding(UTF-8)', $self->file_name
    or $self->logdie("Failed to open Fluidigm export file '",
                     $self->file_name, "': $!");
  my ($header, $column_names, $sample_data) = $self->_parse_fluidigm_table($in);
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

=head2 assay_result_data

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Example    : my $assays = $export->assay_result_data('S01')
  Description: Return a copy of the assay result data for this sample.
               These are the raw output strings from the instrument.
  Returntype : ArrayRef
  Caller     : general

=cut

sub assay_result_data {
  my ($self, $address) = @_;

  return [@{$self->lookup($address)}];
}

=head2 write_assay_result_data

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Arg [2]    : File name
  Example    : $export->write_sample_assays('S01', $file)
  Description: Write a tab-delimited CSV file containing the assay results
               for one sample. Returns the number of records written.
  Returntype : Str
  Caller     : general

=cut

sub write_assay_result_data {
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

  open my $out, '>:encoding(UTF-8)', $file_name
    or $self->logcroak("Failed to open Fluidigm CSV file '$file_name' ",
                       "for writing: $!");

  my @result_data = @{$self->assay_result_data($address)};
  foreach my $record (@result_data) {
    $csv->print($out, $record)
      or $self->logcroak("Failed to write record [", join(", ", @$record),
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

=cut

sub fluidigm_metadata {
  my ($self, $address) = @_;

  defined $address or
    $self->logconfess("A defined address argument is required");
  exists $self->content->{$address} or
    $self->logconfess("FluidigmExportFile '", $self->fluidigm_barcode,
                      "' has no sample address '$address'");

  return ([$self->fluidigm_plate_name_attr => $self->fluidigm_barcode],
          [$self->fluidigm_plate_well_attr => $address]);
}

=head2 fluidigm_fingerprint

  Arg [1]    : Sample address i.e. S01, S02 etc.

  Example    : $export->fluidigm_metadata('S01')
  Description: Return the metadata fingerprint for the sample assayed at
               the address.
  Returntype : ArrayRef

=cut

sub fluidigm_fingerprint {
  my ($self, $address) = @_;

  defined $address or
    $self->logconfess("A defined address argument is required");

  return $self->fluidigm_metadata($address);
}

sub _parse_fluidigm_table {
  my ($self, $fh) = @_;

  # True if we are in the header lines from 'Chip Run Info' to 'Allele
  # Axis Mapping' inclusive
  my $in_header = 0;
  # True if we are in the unique column names row above the sample
  # block
  my $in_column_names = 0;
  # True if we are past the header and into a data block
  my $in_sample_block = 0;

  # Arrays of sample data lines keyed on Chamber IDs
  my %sample_data;

  # For error reporting
  my $line_num = 0;
  my $expected_num_columns = 12;
  my $num_sample_rows = 0;

  my @header;
  my @column_names;

  while (my $line = <$fh>) {
    ++$line_num;
    chomp($line);
    next if $line =~ m{^\s*$}msx;

    if ($line =~ m{^Chip\sRun\sInfo}msx) { $in_header = 1 }
    if ($line =~ m{^Experiment}msx)    { $in_header = 0 }
    if ($line =~ m{^ID}msx)            { $in_column_names = 1 }
    if ($line =~ m{^S\d+\-[[:upper:]]\d+}msx) {
      $in_column_names = 0;
      $in_sample_block = 1;
    }

    if ($in_header) {
      push(@header, $line);
      next;
    }

    if ($in_column_names) {
      @column_names = map { trim($_) } split(',', $line);
      my $num_columns = scalar @column_names;
      unless ($num_columns == $expected_num_columns) {
        $self->logconfess("Parse error: expected $expected_num_columns ",
                          "columns, but found $num_columns at line $line_num");
      }
      next;
    }

    if ($in_sample_block) {
      my @columns = map { trim($_) } split(',', $line);
      my $num_columns = scalar @columns;
      unless ($num_columns == $expected_num_columns) {
        $self->logconfess("Parse error: expected $expected_num_columns ",
                          "columns, but found $num_columns at line $line_num");
      }

      my $id = $columns[0];
      my ($sample_address, $assay_num) = split('-', $id);
      unless ($sample_address) {
        $self->logconfess("Parse error: no sample address in '$id' ",
                          "at line $line_num");
      }
      unless ($assay_num) {
        $self->logconfess("Parse error: no assay number in '$id' ",
                          "at line $line_num");
      }

      if (! exists $sample_data{$sample_address}) {
        $sample_data{$sample_address} = [];
      }

      push(@{$sample_data{$sample_address}}, \@columns);
      $num_sample_rows++;
      next;
    }
  }

  unless (@header) {
    $self->logconfess("Parse error: no header rows found");
  }
  unless (@column_names) {
    $self->logconfess("Parse error: no column names found");
  }

  if ($num_sample_rows == (96 * 96)) {
    unless (scalar keys %sample_data == 96) {
      $self->logconfess("Parse error: expected data for 96 samples, found ",
                        scalar keys %sample_data);
    }
  }
  elsif ($num_sample_rows == (192 * 24)) {
    unless (scalar keys %sample_data == 192) {
      $self->logconfess("Parse error: expected data for 192 samples, found ",
                        scalar keys %sample_data);
    }
  }
  else {
    $self->logconfess("Parse error: expected ", 96 * 96, " or ", 192 * 24,
                      " sample data rows, found $num_sample_rows");
  }

  return (\@header, \@column_names, \%sample_data);
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
    (file_name => 'results.csv');

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

Copyright (C) 2013, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
