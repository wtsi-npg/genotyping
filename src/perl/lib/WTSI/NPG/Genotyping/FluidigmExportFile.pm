
package WTSI::NPG::Genotyping::FluidigmExportFile;

use Moose;
use namespace::autoclean;

use WTSI::NPG::Genotyping::Publication qw(parse_fluidigm_table);

our $HEADER_BARCODE_ROW = 0;
our $HEADER_BARCODE_COL = 2;

our $HEADER_CONF_THRESHOLD_ROW = 5;
our $HEADER_CONF_THRESHOLD_COL = 1;

with 'WTSI::NPG::Loggable';

has 'file_name' => (is  => 'ro', isa => 'Str', required => 1,
                    writer => '_file_name');

has 'header' => (is  => 'ro', isa => 'ArrayRef[Str]',
                 writer => '_header');

has 'column_names' => (is => 'ro', isa => 'ArrayRef[Str]',
                       writer => '_column_names');

has 'fluidigm_barcode' => (is => 'ro', isa => 'Str',
                           builder => '_fluidigm_barcode', lazy => 1);

has 'confidence_threshold' => (is => 'ro', isa => 'Str',
                               builder => '_confidence_threshold', lazy => 1);

has 'num_samples' => (is => 'ro', isa => 'Int',
                      builder => '_num_samples', lazy => 1);

has 'sample_addresses' => (is => 'ro', isa => 'ArrayRef[Str]',
                           builder => '_sample_addresses', lazy => 1);

has 'sample_data' => (is => 'ro', isa => 'HashRef[ArrayRef]',
                      writer => '_sample_data');

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 1 && !ref $args[0]) {
    return $class->$orig(file_name => $args[0]);
  }
  else {
    return $class->$orig(@args);
  }
};

sub BUILD {
  my ($self) = @_;

  unless (-e $self->file_name) {
   $self->logdie("Fluidigm export file '", $self->file_name,
                 "' does not exist");
  }

  open my $in, '<:encoding(utf8)', $self->file_name
    or $self->logdie("Failed to open Fluidigm export file '",
                     $self->file_name, "': $!");

  my ($header, $column_names, $sample_data) = parse_fluidigm_table($in);
  close $in;

  $self->_header($header);
  $self->_column_names($column_names);
  $self->_sample_data($sample_data);
}

sub _fluidigm_barcode {
  my ($self) = @_;

  my @header = @{$self->header};
  my @fields = split ',', $header[$HEADER_BARCODE_ROW];

  return $fields[$HEADER_BARCODE_COL];
}

sub _confidence_threshold {
  my ($self) = @_;

  my @header = @{$self->header};
  my @fields = split ',', $header[$HEADER_CONF_THRESHOLD_ROW];

  return $fields[$HEADER_CONF_THRESHOLD_COL];
}

sub _num_samples {
  my ($self) = @_;

  return scalar keys %{$self->sample_data};
}

sub _sample_addresses {
  my ($self) = @_;

  return [sort keys %{$self->sample_data}];
}

=head2 sample_assays

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Example    : my $assays = $export->sample_assays('S01')
  Description: Return a copy of the assay results for this sample
  Returntype : ArrayRef
  Caller     : general

=cut

sub sample_assays {
  my ($self, $sample_address) = @_;

  defined $sample_address or
    logconfess("A defined sample_address argument is required");

  unless(exists $self->sample_data->{$sample_address}) {
    $self->logcroak("FluidigmExportFile '", $self->fluidigm_barcode,
                    "' has no sample address '$sample_address'");
  }

  return [@{$self->sample_data->{$sample_address}}];
}

=head2 write_sample_assays

  Arg [1]    : Sample address i.e. S01, S02 etc.
  Arg [2]    : File name
  Example    : $export->write_sample_assays('S01', $file)
  Description: Writes a tab-delimited CSV file containing the assay results
               for one sample. Returns the name of the written file.
  Returntype : Str
  Caller     : general

=cut

sub write_sample_assays {
  my ($self, $sample_address, $file_name) = @_;

  defined $sample_address or
    logconfess("A defined sample_address argument is required");
  defined $file_name or logconfess("A defined file_name argument is required");

  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});

  $csv->column_names($self->column_names);

  open(my $out, '>:encoding(utf8)', $file_name)
    or $self->logcroak("Failed to open Fluidigm CSV file '$file_name'",
                       " for writing: $!");

  my @assays = @{$self->sample_assays($sample_address)};
  foreach my $assay (@assays) {
    $csv->print($out, $assay)
      or $self->logcroak("Failed to write record [", join(", ", @$assay),
                         "] to '$file_name': ", $csv->error_diag);
  }

  close($out);

  return $file_name;
}

__PACKAGE__->meta->make_immutable;

no Moose;

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
