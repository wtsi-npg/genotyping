use utf8;

package WTSI::NPG::Genotyping::VCF::Header;

use DateTime;
use Moose;
use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'sample_names' => (
    is             => 'ro',
    isa            => 'ArrayRef[Str]',
    required       => 1,
);

has 'chromosome_lengths' => (
    is            => 'ro',
    isa           => 'HashRef[Int]',
    documentation => 'Used to create contig tags as required by bcftools'
);

has 'source' => (
    is            => 'ro',
    isa           => 'Str',
    default       => 'WTSI_NPG_genotyping_pipeline',
    documentation => 'Standard VCF field to identify the data source'
);

our $VERSION = '';

our $VCF_VERSION = 'VCFv4.0'; # version of VCF format in use
our $INFO = '<ID=ORIGINAL_STRAND,Number=1,Type=String,'.
            'Description="Direction of strand in input file">';
our @FORMAT = (
    '<ID=GT,Number=1,Type=String,Description="Genotype">',
    '<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
    '<ID=DP,Number=1,Type=Integer,Description="Read Depth">',
);
our @COLUMN_HEADS = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;

=head2 str

  Arg [1]    : None

  Example    : $head_string = $header->str()
  Description: Return a string for output as the header of a VCF file.
  Returntype : Str

=cut

sub str {
    my ($self) = @_;
    my @header;
    push @header, '##fileformat='.$VCF_VERSION;
    my $date = DateTime->now(time_zone=>'local')->ymd('');
    push @header, '##fileDate='.$date;
    push @header, '##source='.$self->source;
    if (defined($self->chromosome_lengths)) {
        my @chromosomes = sort(keys(%{$self->chromosome_lengths}));
        foreach my $chr (@chromosomes) {
            my $len = $self->chromosome_lengths->{$chr};
            my $contig = '##contig=<ID='.$chr.
                ',length='.$self->chromosome_lengths->{$chr}.
                ',species="Homo sapiens">';
            push @header, $contig;
        }
    }
    push @header, '##INFO='.$INFO;
    foreach my $format_field (@FORMAT) {
        push @header, '##FORMAT='.$format_field;
    }
    my @colHeads = @COLUMN_HEADS;
    push @colHeads, @{$self->sample_names};
    push @header, "#".join "\t", @colHeads;
    return join "\n", @header;
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::Header

=head1 DESCRIPTION

Class to represent the header of a VCF file. Has general information for a
dataset, including sample identifiers. Information on each variant (eg. SNP)
is contained in DataRow objects.

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
