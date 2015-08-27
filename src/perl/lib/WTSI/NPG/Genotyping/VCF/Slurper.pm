use utf8;

package WTSI::NPG::Genotyping::VCF::Slurper;

use Moose;

use WTSI::NPG::Genotyping::VCF::DataRowParser;
use WTSI::NPG::Genotyping::VCF::HeaderParser;
use WTSI::NPG::Genotyping::VCF::VCFDataSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

with 'WTSI::DNAP::Utilities::Loggable';

has 'snpset'  =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::SNPSet',
    builder       => '_build_snpset',
    documentation => 'SNPSet containing the variants in the VCF input',
    lazy          => 1,
   );

has 'input_filehandle' =>
   (is            => 'ro',
    isa           => 'FileHandle',
    required      => 1,
    documentation => 'Filehandle for input of VCF data.',
   );

has 'irods'      =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   default       => sub { return WTSI::NPG::iRODS->new },
   documentation => 'An iRODS handle');

has 'header' =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::VCF::Header',
    builder       => '_build_header',
    documentation => 'Object representing the VCF header',
    lazy          => 1,
   );

has 'sample_names' =>
   (is             => 'ro',
    isa            => 'ArrayRef[Str]',
    documentation  => 'Optional array of sample names. If given, will '.
                      'override names read from the VCF header.'
   );

has 'snpset_irods_paths' =>
   (is             => 'ro',
    isa            => 'HashRef[HashRef[Str]]',
    documentation  => "Optional hashref containing snpset iRODS paths by ".
                      "platform name (eg. 'sequenom', 'fluidigm') and ".
                      "snpset name (eg. 'W30467', 'qc')."
   );


our $VERSION = '';

# can supply a JSON config file instead of SNPSet object
# uses config to look up snpset from metadata in VCF header
# alternatively, just use hard-coded defaults

sub BUILD {
    my ($self) = @_;
    my $header = $self->header; # ensure header is read first
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
    my ($self) = @_;
    my $rowParser = WTSI::NPG::Genotyping::VCF::DataRowParser->new(
        input_filehandle => $self->input_filehandle,
        snpset => $self->snpset,
    );
    my $rows = $rowParser->get_all_remaining_rows();
    return WTSI::NPG::Genotyping::VCF::VCFDataSet->new(
        header => $self->header,
        data   => $rows,
    );
}

sub _build_header {
    my ($self) = @_;
    my %hpArgs = (  input_filehandle => $self->input_filehandle );
    if ($self->sample_names) {
        $hpArgs{'sample_names'} = $self->sample_names;
    }
    my $headerParser = WTSI::NPG::Genotyping::VCF::HeaderParser->new(
        %hpArgs);
    return $headerParser->header();
}

sub _build_snpset {
    # use metadata from header to look up snpset location in iRODS
    my ($self) = @_;
    unless (defined($self->snpset_irods_paths)) {
        $self->logcroak("Cannot build snpset from VCF header without a ",
                        "snpset_paths attribute");
    }
    my %metadata = %{$self->header->metadata};

    if (!defined($metadata{'plex_type'}) ||
            !defined($metadata{'plex_name'})) {
        $self->logcroak("Cannot build snpset from VCF header without ",
                        "plex_type and plex_name entries");
    }
    my @plex_types =  @{$metadata{'plex_type'}};
    if (scalar @plex_types > 1) {
        $self->logcroak("Cannot have more than one plex type in VCF header");
    }
    my @plex_names =  @{$metadata{'plex_name'}};
    if (scalar @plex_names > 1) {
        $self->logcroak("Cannot have more than one plex name in VCF header");
    }
    my $plex_type = shift @plex_types;
    my $plex_name = shift @plex_names;
    my $snpset_path = $self->snpset_irods_paths->{$plex_type}->{$plex_name};
    my $snpset_obj = WTSI::NPG::iRODS::DataObject->new(
        $self->irods,
        $snpset_path);
    return WTSI::NPG::Genotyping::SNPSet->new($snpset_obj);
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
