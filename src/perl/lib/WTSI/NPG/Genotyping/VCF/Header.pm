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
    documentation => 'Used to create contig tags as required by bcftools. '.
                     'In typical usage, each contig corresponds to a homo '.
                     'sapiens chromosome.'
);

has 'source' => (
    is            => 'ro',
    isa           => 'Str',
    default       => 'WTSI_NPG_genotyping_pipeline',
    documentation => 'Standard VCF field to identify the data source'
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
our $INFO = '<ID=ORIGINAL_STRAND,Number=1,Type=String,'.
            'Description="Direction of strand in input file">';
our @FORMAT = (
    '<ID=GT,Number=1,Type=String,Description="Genotype">',
    '<ID=GQ,Number=1,Type=Integer,Description="Genotype Quality">',
    '<ID=DP,Number=1,Type=Integer,Description="Read Depth">',
);
our @COLUMN_HEADS = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;

around BUILDARGS => sub {
    # optionally, populate contig_lengths attribute using an ArrayRef[Str]
    # with the key 'contig_strings'. Not compatible with supplying a value
    # for contig_lengths.
    my ($orig, $class, @args) = @_;
    my %init_args = @args;
    my %new_args;
    if (defined($init_args{'contig_strings'})) {
        if (!is_ArrayRef($init_args{'contig_strings'})) {
            $class->logcroak("'contig_strings' argument to Header ",
                             "must be an ArrayRef[Str]");
        } elsif (defined($init_args{'contig_lengths'})) {
            $class->logcroak("Cannot supply both contig_lengths and ",
                             "contig_strings arguments");
        }
        my %contig_lengths = ();
        foreach my $str (@{$init_args{'contig_strings'}}) {
            if (!is_Str($str)) {
                $class->logcroak("Values of contig_strings ArrayRef ",
                                 "must be strings");
            }
            # cannot access $self->species before object creation
            my $species = $init_args{'species'} || $DEFAULT_SPECIES;
            my ($contig, $length) = $class->string_to_contig($str, $species);
            $contig_lengths{$contig} = $length;
        }
        $new_args{'contig_lengths'} = \%contig_lengths;
        foreach my $key (keys %init_args) {
            if ($key ne 'contig_strings') {
                $new_args{$key} = $init_args{$key};
            }
        }
        return \%new_args;
    } else {
        return $class->$orig(@_);
    }
};

=head2 contig_to_string

  Arg [1]    : [Str] Contig name
  Arg [2]    : [Int] Contig length

  Example    : my $contig_string = $contig_to_string($name, $length)
  Description: Generate a contig string for the VCF header. For now,
               this package requires contigs to be human chromosomes.
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
    my $contig_str = '##contig=<ID='.$name.',length='.$length.
        ',species="'.$self->species.'">';
    return $contig_str;
}

=head2 string_to_contig

  Arg [1]    : Contig string

  Example    : my ($name, $length) = $string_to_contig($contig_str)
  Description: Parse a contig string from the VCF header. The input string
               must be strictly in the format output by contig_to_string.
               For now, this package requires contigs to be human
               chromosomes.

               It was decided to keep all definitions for the format of
               contig lines in the Header class. Therefore, string_to_contig
               is in Header.pm instead of VCFParser.pm.
  Returntype : ArrayRef[Str]

=cut

sub string_to_contig {
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
    push @header, '##fileformat='.$VCF_VERSION;
    my $date = DateTime->now(time_zone=>'local')->ymd('');
    push @header, '##fileDate='.$date;
    push @header, '##source='.$self->source;
    if (scalar keys $self->contig_lengths > 0) {
        my @contigs = sort(keys(%{$self->contig_lengths}));
        foreach my $contig (@contigs) {
            my $contig_string = $self->contig_to_string(
                $contig,
                $self->contig_lengths->{$contig});
            push @header, $contig_string;
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

=head1 ARGUMENTS

=over 1

=item * Sample names: Required. An ArrayRef[Str] of sample identifiers.

=item * Contig lengths: Optional. May supply one (but not both) of:

=over 2

=item * contig_strings, an ArrayRef[Str] of contig lines from a VCF file header

=item * contig_lengths, a HashRef[Int] of contig names and lengths

=back

=item * Data source: Optional. A value for the 'source' field in the header output.

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
