
use utf8;

package WTSI::NPG::Expression::ChipLoadingManifestV2;

use List::AllUtils qw(firstidx);
use Moose;

use WTSI::NPG::Utilities qw(trim);

our $VERSION = '';

extends 'WTSI::NPG::Expression::ChipLoadingManifest';

sub BUILD {
  my ($self) = @_;

  -e $self->file_name or
    $self->logdie("Manifest file '", $self->file_name,
                  "' does not exist");

  open my $in, '<:encoding(UTF-8)', $self->file_name
    or $self->logdie("Failed to open Manifest file '",
                     $self->file_name, "': $!");

  $self->samples($self->_parse_beadchip_table($in));
  close $in;
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
sub _parse_beadchip_table {
  my ($self, $fh) = @_;

  # Channel is always Grn (Cy3)
  my $channel = 'Grn';

  # For error reporting
  my $n = 0;

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
    ++$n;
    chomp($line);
    next if $line =~ m/^\s*$/;

    if ($in_sample_block) {
      my @row = map { trim($_) } split("\t", $line);

      if ($row[$end_of_data_col] =~ /^Kit Control$/i) {
        last;
      }
      else {
        my $sample = $self->_validate_sample_id($row[$sample_id_col], $n);
        my $plate = $self->_validate_plate_id($row[$supplier_plate_id_col], $n);
        my $well = $self->_validate_well_id($row[$supplier_well_id_col], $n);
        my $beadchip = $self->_validate_beadchip($row[$beadchip_col], $n);
        my $section = $self->_validate_section($row[$section_col], $n);

        push @samples, {sample_id        => $sample,
                        plate_id         => $plate,
                        well_id          => $well,
                        beadchip         => $beadchip,
                        beadchip_section => $section};
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

  foreach my $sample (@samples) {
    my $basename = sprintf("%s_%s_%s",
                           $sample->{beadchip},
                           $sample->{beadchip_section},
                           $channel);

    $sample->{idat_file} = $basename . '.idat';
    $sample->{xml_file}  = $basename . '.xml' ;
  }

  return \@samples;
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
