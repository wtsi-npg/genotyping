use utf8;

package WTSI::NPG::Genotyping::VCF::Slurper;

use Moose;

use File::Temp qw /tempdir/;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Genotyping::VCF::DataRowParser;
use WTSI::NPG::Genotyping::VCF::HeaderParser;

with 'WTSI::DNAP::Utilities::Loggable';

has 'input_path'  =>
   (is            => 'ro',
    isa           => 'Str',
    required      => 1,
    documentation => "Input path in iRODS or local filesystem,".
                     " or '-' for STDIN.",
   );

has 'irods'       =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::iRODS',
    documentation => '(Optional) iRODS instance from which to read data. '.
                     'If not given, input (if any) is assumed to be a '.
                     'path on the local filesystem.',
   );

has 'snpset'  =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::SNPSet',
    required      => 1,
    documentation => 'SNPSet containing the variants in the VCF input',
   );

has '_input_filehandle' =>
   (is            => 'ro',
    isa           => 'FileHandle',
    lazy          => 1,
    builder       => '_build_input_filehandle',
    documentation => 'Private filehandle for input of VCF data.',
    init_arg      => undef, # cannot set at creation time
   );

our $VERSION = '';

sub DEMOLISH {
    # ensure filehandle is closed when VCFReader object goes out of scope
    my ($self) = @_;
    if ($self->input_path ne '-') {
        close $self->_input_filehandle ||
            $self->logcroak("Failed to close VCF input filehandle");
    }
}

=head2 read_dataset

  Arg [1]    : None
  Example    : my $dataset = $slurper->read_dataset();
  Description: Read a complete VCF dataset from the given input filehandle.
               Throws an error if filehandle is not positioned at the start
               of the VCF header.
  Returntype : WTSI::NPG::Genotyping::VCF::VCFDataSet

=cut

sub read_dataset {
    # TODO if iRODS is defined, read sample names from iRODS metadata and supply as argument to header parser
    my ($self) = @_;
    my $headerParser = WTSI::NPG::Genotyping::VCF::HeaderParser->new(
        input_filehandle => $self->_input_filehandle,
    );
    my $header = $headerParser->header();
    my $rowParser = WTSI::NPG::Genotyping::VCF::DataRowParser->new(
        input_filehandle => $self->_input_filehandle,
        snpset => $self->snpset,
    );
    my $rows = $rowParser->get_all_remaining_rows();
    return WTSI::NPG::Genotyping::VCF::VCFDataSet->new(
        header => $header,
        data   => $rows,
    );
}

sub _build_input_filehandle {
    # allows input from STDIN, iRODS or local file
    my ($self) = @_;
    my $filehandle;
    if ($self->input_path eq '-') {
        # Moose FileHandle requires a reference, not a typeglob
        $filehandle = \*STDIN;
    } else {
        my $localInputPath;
        if ($self->irods) {
            my $tmpdir = tempdir('vcf_parser_irods_XXXXXX', CLEANUP => 1);
            $localInputPath = "$tmpdir/input.vcf";
            $self->irods->get_object($self->input_path, $localInputPath);
        } else {
            $localInputPath = $self->input_path;
        }
        open $filehandle, "<", $localInputPath ||
            $self->logcroak("Cannot open input path '",
                            $localInputPath, "'");
    }
    return $filehandle;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::Slurper

=head1 DESCRIPTION

Convenience class to slurp a VCF file from iRODS, standard input, or a
regular file, and return a VCFDataSet object.

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
