use utf8;

package WTSI::NPG::Genotyping::VCF::Slurper;

use Moose;

use WTSI::NPG::Genotyping::VCF::DataRowParser;
use WTSI::NPG::Genotyping::VCF::HeaderParser;
use WTSI::NPG::Genotyping::VCF::VCFDataSet;

with 'WTSI::DNAP::Utilities::Loggable';

has 'snpset'  =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::SNPSet',
    required      => 1,
    documentation => 'SNPSet containing the variants in the VCF input',
   );

has 'input_filehandle' =>
   (is            => 'ro',
    isa           => 'FileHandle',
    required      => 1,
    documentation => 'Filehandle for input of VCF data.',
   );

has 'sample_names' =>
   (is             => 'ro',
    isa            => 'ArrayRef[Str]',
    documentation  => 'Optional array of sample names. If given, will '.
                      'override names read from the VCF header.'
   );

our $VERSION = '';


=head2 read_dataset

  Arg [1]    : None
  Example    : my $dataset = $slurper->read_dataset();
  Description: Read a complete VCF dataset from the given input filehandle.
               Throws an error if filehandle is not positioned at the start
               of the VCF header.
  Returntype : WTSI::NPG::Genotyping::VCF::VCFDataSet

=cut

sub read_dataset {
    my ($self) = @_;
    my %hpArgs = (  input_filehandle => $self->input_filehandle );
    if ($self->sample_names) {
        $hpArgs{'sample_names'} = $self->sample_names;
    }
    my $headerParser = WTSI::NPG::Genotyping::VCF::HeaderParser->new(
        %hpArgs);
    my $header = $headerParser->header();
    my $rowParser = WTSI::NPG::Genotyping::VCF::DataRowParser->new(
        input_filehandle => $self->input_filehandle,
        snpset => $self->snpset,
    );
    my $rows = $rowParser->get_all_remaining_rows();
    return WTSI::NPG::Genotyping::VCF::VCFDataSet->new(
        header => $header,
        data   => $rows,
    );
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::Slurper

=head1 DESCRIPTION

Convenience class to slurp a VCF file from a filehandle, and return a
VCFDataSet object.

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
