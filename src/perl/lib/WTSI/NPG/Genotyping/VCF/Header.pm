use utf8;

package WTSI::NPG::Genotyping::VCF::Header;

use DateTime;
use Moose;
use MooseX::Types::Moose qw(ArrayRef Str);
use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'sample_names' => (
    is             => 'ro',
    isa            => 'ArrayRef[Str]',
    required       => 1,
);

has 'contig_lengths' => (
    is            => 'ro',
    isa           => 'HashRef[Int]',
    documentation => 'Used to create contig metadata required by bcftools. '.
                     'In typical usage, each contig corresponds to a homo '.
                     'sapiens chromosome.',
);

has 'metadata' => (
    is            => 'ro',
    isa           => 'HashRef[ArrayRef[Str]]',
    documentation => 'Metadata contained in the VCF header, represented '.
                     'as key/value pairs. Each value is an array of one or '.
                     'more strings. At a minimum, metadata should include '.
                     'source, reference, INFO and FORMAT entries. '.
                     '(Defaults will be assigned if not given.) '.
                     'See the VCF specification for other reserved keys.',
);

our $DEFAULT_SPECIES = 'Homo sapiens';

has 'species' => (
    is            => 'ro',
    isa           => 'Str',
    default       => $DEFAULT_SPECIES,
    documentation => 'Species identifier for use in contigs'
);

our $VERSION = '';

our $VCF_VERSION = 'VCFv4.2'; # version of VCF format in use

our $FF_KEY = 'fileformat';
our $DATE_KEY ='fileDate';
our $CONTIG_KEY = 'contig';
our $SOURCE_KEY = 'source';
our $REFERENCE_KEY = 'reference';
our @RESERVED_KEYS = ($FF_KEY, $DATE_KEY, $SOURCE_KEY, $REFERENCE_KEY,
                      $CONTIG_KEY);
push @RESERVED_KEYS, qw/INFO FILTER FORMAT ALT SAMPLE PEDIGREE assembly/;
our $SOURCE_DEFAULT = 'WTSI_NPG_genotyping_pipeline';
our @COLUMN_HEADS = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;

sub BUILD {
    # check for required fields in metadata, add defaults if needed
    my ($self) = @_;
    my %defaults = (
        $FF_KEY => [
            $VCF_VERSION,
        ],
        $DATE_KEY => [
            DateTime->now(time_zone=>'local')->ymd(''),
        ],
        INFO => [
            '<ID=ORIGINAL_STRAND,Number=1,Type=String,'.
                'Description="Direction of strand in input file">',
        ],
        FORMAT => [
            '<ID=GT,Number=1,Type=String,Description="Genotype">',
            '<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
            '<ID=DP,Number=1,Type=Integer,Description="Read Depth">',
        ],
        $SOURCE_KEY => [
            $SOURCE_DEFAULT,
        ],
        $REFERENCE_KEY => [
            'unknown',
        ]
    );
    # if contig lengths are available, add to defaults
    if (defined $self->contig_lengths
	&& scalar keys %{$self->contig_lengths} > 0) {
        my @contig_values;
        my @contigs = sort(keys(%{$self->contig_lengths}));
        foreach my $contig (@contigs) {
            my $contig_value = $self->contig_to_string(
                $contig,
                $self->contig_lengths->{$contig});
            push @contig_values, $contig_value;
        }
        $defaults{'contig'} = \@contig_values;
    }
    # merge defaults into given metadata where required
    foreach my $key (keys %defaults) {
        if (!defined($self->metadata->{$key})) {
            $self->metadata->{$key} = $defaults{$key};
        }
    }
}

=head2 contig_to_string

  Arg [1]    : [Str] Contig name
  Arg [2]    : [Int] Contig length

  Example    : my $contig_string = $contig_to_string($name, $length)
  Description: Generate a contig string for the VCF header. For now,
               this package requires contigs to be human chromosomes.
               String does not include the key prefix '##contig='.
  Returntype : Str

=cut

sub contig_to_string {
    my ($self, $name, $length) = @_;
    if (!is_HsapiensChromosome($name)) {
        $self->logcroak("Contig name must be a homo sapiens chromosome");
    } elsif ($name =~ m/^[Cc]hr/msx) {
        $self->logcroak("'Chr' or 'chr' prefix is not permitted");
    } elsif (!is_PositiveInt($length)) {
        $self->logcroak("Contig length must be an integer > 0");
    }
    my $contig_str = '<ID='.$name.',length='.$length.
        ',species="'.$self->species.'">';
    return $contig_str;
}

=head2 parse_contig_line

  Arg [1]    : Contig line
  Arg [2]    : Species (optional, defaults to 'species' attribute)

  Example    : my ($name, $length) = $parse_contig_line($contig_str)
  Description: Parse a contig line from the VCF header. The input string is
               of the form ##contig=$VALUE', where $VALUE must be strictly
               in the format output by contig_to_string.
               For now, this package requires contigs to be human
               chromosomes.

               It was decided to keep all definitions for the format of
               contig lines in the Header class. Therefore, parse_contig_line
               is in Header.pm instead of VCFParser.pm.
  Returntype : ArrayRef[Str]

=cut

sub parse_contig_line {
    # Deals defensively with awkward VCF contig tag format, by requiring
    # a strict match with the output of contig_to_string
    my ($self, $input, $species) = @_;
    chomp $input;
    $species ||= $self->species;
    my ($name, $length);
    my $species_quoted = quotemeta($species); # escapes all metacharacters
    my $pattern = '^[#]{2}contig=[<]ID=(X|Y|\d{1,2}),length=\d+,'.
        'species=\"'.$species_quoted.'\"[>]$';
    if ($input =~ qr/$pattern/msx) {
        my @terms = split /[<>,]/msx, $input;
        foreach my $term (@terms) {
            my ($key, $value) = split /=/msx, $term;
            if ($term =~ /^ID=([XY]|\d{1,2})$/msx) {
                $name = $value;
            } elsif ($term =~ /^length=\d+$/msx) {
                $length = $value;
            }
        }
    } else {
        $self->logcroak("Unknown contig format in VCF header string: '",
                        $input, "'");
    }
    unless (defined($name) && defined($length)) {
        $self->logcroak("Unable to parse contig name and length ",
                        "from string '", $input, "'");
    }
    return ($name, $length);
}

=head2 str

  Arg [1]    : None

  Example    : $head_string = $header->str()
  Description: Return a string for output as the header of a VCF file.
  Returntype : Str

=cut

sub str {
    my ($self) = @_;
    my @header;
    # metadata
    my %res_key_hash;
    foreach my $key (@RESERVED_KEYS) {
        $res_key_hash{$key} = 1;
        my $valuesRef = $self->metadata->{$key};
        unless (defined($valuesRef)) { next; }
        foreach my $value (@{$valuesRef}) {
            push @header, '##'.$key.'='.$value;
        }
    }
    my @all_keys = sort(keys %{$self->metadata});
    foreach my $key (@all_keys) {
        # append non-reserved key/value pairs (if any)
        if ($res_key_hash{$key}) {
            next;
        }
        foreach my $value (@{$self->metadata->{$key}}) {
            push @header, '##'.$key.'='.$value;
        }
    }
    # column headers
    my @colHeads = @COLUMN_HEADS;
    push @colHeads, @{$self->sample_names};
    push @header, "#".join "\t", @colHeads;
    return join "\n", @header;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::Header

=head1 DESCRIPTION

Class to represent the header of a VCF file. Has general information for a
dataset, including sample identifiers. Information on each variant (eg. SNP)
is contained in DataRow objects.

=head1 ARGUMENTS

=over 1

=item * Sample names: Required. An ArrayRef[Str] of sample identifiers.

=item * Contig lengths: Optional. May supply one (but not both) of:

=over 2

=item * contig_strings, an ArrayRef[Str] of contig lines from a VCF file header

=item * contig_lengths, a HashRef[Int] of contig names and lengths

=back

=item * Metadata: Optional. A HashRef[ArrayRef[Str]] of metadata fields for the VCF header. Any required fields not supplied receive default values.

=item * Species: Optional. Species identifier for contig lines in VCF input or output. Defaults to 'Homo sapiens'.

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
