use utf8;

package WTSI::NPG::Genotyping::VCF::DataRowParser;

use Moose;

extends 'WTSI::NPG::Genotyping::VCF::Parser';

use MooseX::Types::Moose qw(Int);
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::VCF::DataRow;

with 'WTSI::DNAP::Utilities::Loggable';

has 'snpset'  =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::SNPSet',
    required      => 1,
    documentation => 'SNPSet containing the variants in the VCF input',
   );

our $VERSION = '';

=head2 get_next_data_row

  Arg [1]    : None

  Example    : $row = parser->get_next_data_row
  Description: Get the next data row from the VCF file, or return undef if
               EOF has been reached. Skips past any header lines which
               appear before the next data row.
  Returntype : Maybe[WTSI::NPG::Genotyping::VCF::DataRow]

=cut

sub get_next_data_row {
    my ($self) = @_;
    my $dataRow;
    my $searching = 1;
    while ($searching) {
        my $line = readline $self->input_filehandle;
        if (!$line) { # EOF reached
            $searching = 0;
        } elsif ($line !~ m/^[#]/msx) { # first non-header line
            $dataRow = $self->_parse_data_row($line);
            $searching = 0;
        }
    }
    return $dataRow;
}


=head2 get_all_remaining_rows

  Arg [1]    : None

  Example    : $rows = parser->get_all_remaining_rows
  Description: Read all remaining data rows up to end of file, and return
               as an ArrayRef. Reads the entire remaining (parsed) contents
               of the input into memory; may not be desirable for large
               input files.
  Returntype : ArrayRef[WTSI::NPG::Genotyping::VCF::DataRow]

=cut

sub get_all_remaining_rows {
    my ($self) = @_;
    my @dataRows;
    my $row = 1;
    while ($row) {
        $row = $self->get_next_data_row();
        if ($row) { push @dataRows, $row; }
    }
    return \@dataRows;
}

sub _call_from_vcf_string {
    # raw call of the form GT:GQ:DP
    # (Other VCF call formats not supported!)
    # GT = genotype
    # GQ = genotype quality (. if absent)
    # DP = read depth
    # arguments:
    # - VCF string
    # - $ref, alt alleles from vcf
    # - WTSI:Genotyping:SNP
    # return a WTSI:Genotyping:Call object
    my ($self, $input, $ref, $alt, $snp) = @_;
    my @terms = split /:/msx, $input;
    if (scalar @terms != 3) {
        $self->logcroak("Need exactly 3 colon-separated ",
                        "terms in call string");
    }
    my ($gt, $gq, $dp) = @terms;
    my ($genotype, $is_call);
    if ($gt eq '.') {
        $genotype = 'NN';
        $is_call = 0;
    } else {
        my $pattern = '[/]|[|]';
        my @gt = split qr/$pattern/msx, $gt;
        if (scalar @gt != 2) {
            $self->logcroak("Found (", join(', ', @gt),
                            "); expected exactly two alleles separated ",
                            "by '/' or '|'");
        }
        # members of @gt are one of (0, 1, .)
        if ($gt[0] eq '.' && $gt[1] eq '.') {
            $genotype = 'NN';
            $is_call = 0;
        } else {
            my @gt_alleles;
            if ($gt[0] eq '0') { push @gt_alleles, $ref; }
            elsif ($gt[0] eq '1') { push @gt_alleles, $alt; }
            else { $self->logcroak("Illegal allele code: '", $gt[0], "'"); }
            if ($gt[1] eq '0') { push @gt_alleles, $ref; }
            elsif ($gt[1] eq '1') { push @gt_alleles, $alt; }
            elsif ($gt[1] eq '.') { push @gt_alleles, $gt[0]; } # homozygote
            else { $self->logcroak("Illegal allele code: '", $gt[0], "'"); }
            $genotype = join '', @gt_alleles;
            $is_call = 1;
        }
    }
    my $qscore;
    if (is_Int($gq)) { $qscore = $gq; }
    return WTSI::NPG::Genotyping::Call->new
        (snp      => $snp,
         genotype => $genotype,
         is_call  => $is_call,
         qscore   => $qscore
     );
}

sub _parse_data_row {
    # parse a line from the main body of a VCF file and create a DataRow
    # required parameters:
    # - qscore of alternate reference allele, if any ('.' otherwise)
    # - filter status ('.' if missing)
    # - additional_info string
    # - ArrayRef of Call objects
    my ($self, $line) = @_;
    my @fields = $self->_split_delimited_string($line);
    my $qscore_raw = $fields[$self->_field_index('QSCORE')];
    my $qscore; # integer qscore if one is defined, undef otherwise
    if (is_Int($qscore_raw)) { $qscore = $qscore_raw; }
    my $snp_name = $fields[$self->_field_index('VARIANT_NAME')];
    my $snp = $self->snpset->named_snp($snp_name);
    # TODO: do (chromosome, position) match in manifest and VCF file?
    # Problem: Different chromosome naming conventions, eg. Chr1 vs. 1
    my @calls;
    my $i = $self->_field_index('SAMPLE_START');
    while ($i < scalar @fields) {
      push(@calls, $self->_call_from_vcf_string(
          $fields[$i],
          $fields[$self->_field_index('REF_ALLELE')],
          $fields[$self->_field_index('ALT_ALLELE')],
          $snp)
       );
      $i++;
    }
    return WTSI::NPG::Genotyping::VCF::DataRow->new
      (qscore          => $qscore,
       filter          => $fields[$self->_field_index('FILTER')],
       additional_info => $fields[$self->_field_index('INFO')],
       calls           => \@calls);
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::DataRowParser

=head1 DESCRIPTION

Subclass of Parser to read DataRow objects from a filehandle. Any lines
beginning with a '#' character are assumed to be part of the header, and will
be ignored. (This means that in principle, DataRowParser can parse the
contents of multiple VCF files concatenated together, skipping the header of
each one.)

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
