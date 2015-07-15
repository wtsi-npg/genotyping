use utf8;

package WTSI::NPG::Genotyping::VCF::HeaderParser;

use Moose;

extends 'WTSI::NPG::Genotyping::VCF::Parser';

use WTSI::NPG::Genotyping::VCF::Header;

with 'WTSI::DNAP::Utilities::Loggable';

has 'sample_names' =>
   (is             => 'ro',
    isa            => 'ArrayRef[Str]',
    documentation  => 'Optional array of sample names. If given, will '.
                      'override names read from the VCF header.'
   );

has 'header'      =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::VCF::Header',
    lazy          => 1,
    builder       => '_build_header',
   );

our $VERSION = '';

sub _build_header {
    # need to find sample names and contig lengths
    # requires input filehandle to be at start of header
    my ($self) = @_;
    # code to parse contig strings is in Header.pm instead of VCFParser.pm
    # (ensures definition of contig string format is all in the same class)
    # so, read the contig lines as strings and supply as alternative argument
    # to Header constructor
    my (@contig_lines, $sample_names, $reference);
    my $in_header = 1;
    my $first = 1;
    while ($in_header) {
        my $line = readline $self->input_filehandle;
        if ($first && $line !~ m/^[#]{2}fileformat=VCF.+/msx) {
            $self->logcroak("VCF header input does not start with ",
                            "a valid ##fileformat line. Incorrect ",
                            "position of filehandle?");
        } else {
            $first = 0;
        }
        if (eof $self->input_filehandle) {
            $self->logcroak("Unexpected EOF while reading header");
        } elsif ($line =~ /^[#]{2}contig/msx ) {
            push @contig_lines, $line;
	} elsif ($line =~ /^[#]{2}reference=/msx) {
	    $reference = _parse_reference($line);
	} elsif ($line =~ /^[#]CHROM/msx ) {
            # last line of header contains sample names
            my $vcf_sample_names = $self->_parse_sample_names($line);
            if ($self->sample_names) {
                my $arg_total = scalar @{$self->sample_names};
                my $vcf_total = scalar @{$vcf_sample_names};
                if ($arg_total != $vcf_total) {
                    $self->logcroak("Inconsistent numbers of sample names: ",
                                $arg_total, " in sample_names argument, ",
                                $vcf_total, " in VCF file header.");
                } else {
                    $sample_names = $self->sample_names;
                }
            } else {
                $sample_names = $vcf_sample_names;
            }
            $in_header = 0;
        }
    }
    return WTSI::NPG::Genotyping::VCF::Header->new
        (sample_names   => $sample_names,
         contig_strings => \@contig_lines,
	 reference      => $reference,
	 );
}

sub _parse_reference {
  # parse the ##reference line of a VCF file
  my ($line) = @_;
  chomp $line;
  my @terms = split /=/msx, $line;
  shift @terms; # remove the '##reference' string
  my $reference = join '=', @terms; # allows '=' characters in reference name
  return $reference;
}

sub _parse_sample_names {
    # parse sample names from the #CHROM line of a VCF file
    my ($self, $line) = @_;
    my @fields = $self->_split_delimited_string($line);
    my $i = $self->_field_index('SAMPLE_START');
    my @sample_names;
    while ($i < scalar @fields) {
        push @sample_names, $fields[$i];
        $i++;
    }
    if (scalar @sample_names < 1) {
        $self->logcroak("No sample names found in VCF header");
    }
    return \@sample_names;
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::HeaderParser

=head1 DESCRIPTION

Subclass of Parser to read a VCF Header object from a filehandle. The given
filehandle must be positioned at the start of the VCF header, so that the
first line read is a VCF '##fileformat' line. If this is not the case, an
error will be raised.

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
