use utf8;

package WTSI::NPG::Genotyping::VCF::Parser;

use Moose::Role;

use Text::CSV;

with 'WTSI::DNAP::Utilities::Loggable';

has 'csv'         =>
   (is            => 'ro',
    isa           => 'Text::CSV',
    default       => sub {
        return Text::CSV->new({sep_char => "\t",
                               binary   => 1 });
    },
    documentation => 'Object to parse tab-delimited input lines'
   );

has 'input_filehandle'  =>
   (is            => 'ro',
    isa           => 'FileHandle',
    required      => 1,
    documentation => 'Filehandle for input of VCF data',
   );

our $VERSION = '';

sub _split_delimited_string {
    # use the CSV attribute to parse a tab delimited string
    # returns an array of strings for the tab delimited fields
    # removes newline (if any) from end of the string before splitting
    my ($self, $string) = @_;
    chomp $string;
    $self->csv->parse($string);
    return $self->csv->fields();
}

sub _field_index {
    # return the index for a named field, raise error on invalid name
    # using a subroutine allows inheritance in subclasses
    my ($self, $name) = @_;
    my %indices = (
        CHROMOSOME   => 0,
        POSITION     => 1,
        VARIANT_NAME => 2,
        REF_ALLELE   => 3,
        ALT_ALLELE   => 4,
        QSCORE       => 5,
        FILTER       => 6,
        INFO         => 7,
        FORMAT       => 8,
        SAMPLE_START => 9,
    );
    if (defined($indices{$name})) {
        return $indices{$name};
    } else {
        $self->logcroak("Invalid name for VCF field: '", $name, "'");
    }
}

no Moose;

1;


__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF:Parser

=head1 DESCRIPTION

Base class for parsing VCF files. Not intended to be instantiated, parsing
is done by subclasses such as HeaderParser and DataRowParser. Parser includes
methods and attributes for use in subclasses.

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
