
use utf8;

package WTSI::NPG::Genotyping::VCF::AssayResultParser;

use JSON;
use List::AllUtils qw(uniq);
use Log::Log4perl::Level;
use Moose;

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::GenderMarkerCall;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::Genotyping::Types qw(:all);
use WTSI::NPG::Genotyping::VCF::DataRow;
use WTSI::NPG::Genotyping::VCF::Header;
use WTSI::NPG::Genotyping::VCF::VCFDataSet;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '';
our $SEQUENOM_TYPE = 'sequenom'; # TODO remove redundancy wrt vcf_from_plex.pl
our $FLUIDIGM_TYPE = 'fluidigm';

has 'contig_lengths' => # must be compatible with given snpset
   (is            => 'ro',
    isa           => 'HashRef[Int]',
    required      => 1,
    documentation => 'Used to generate contig tags required by bcftools. '.
                     'In typical usage, each contig corresponds to a '.
                     'homo sapiens chromosome.',
   );

has 'resultsets'  =>
   (is            => 'ro',
    isa           => ArrayRefOfResultSet,
    required      => 1,
    documentation => 'Array of AssayResultSets used to find sample '.
                     'names and calls.',
   );

has 'snpset'     =>
   (is           => 'ro',
    isa          => 'WTSI::NPG::Genotyping::SNPSet',
    required     => 1,
   );

has 'metadata' =>
   (is            => 'ro',
    isa           => 'HashRef[ArrayRef[Str]]',
    documentation => 'Additional metadata fields to populate the VCF header',
    default       => sub { {} },
   );


sub BUILD {
    my ($self) = @_;
    if (scalar @{$self->resultsets} == 0) {
        $self->logcroak("Must have at least one AssayResultSet input!");
    }
}

=head2 get_vcf_dataset

  Arg [1]    : None

  Example    : my $vcf_data = $reader->get_vcf_dataset();
  Description: Create a VCFDataSet object using the given AssayResultSets.
  Returntype : WTSI::NPG::Genotyping::VCF::VCFDataSet

=cut

sub get_vcf_dataset {
    my ($self) = @_;
    my ($calls, $samples) = $self->_parse_assay_results();
    my $header = WTSI::NPG::Genotyping::VCF::Header->new(
        sample_names => $samples,
        contig_lengths => $self->contig_lengths,
	metadata => $self->metadata,
    );
    my @rows;
    foreach my $snp (@{$self->snpset->snps}) {
        my @sample_calls;
        foreach my $sample (@$samples) {
            unless (defined($calls->{$snp->name}{$sample})) {
                $self->logcroak("No call found for SNP '", $snp->name,
                                "' sample '", $sample, "'");
            }
            push @sample_calls, $calls->{$snp->name}{$sample};
        }
        if (is_GenderMarker($snp)) {
            my ($x_row, $y_row) = $self->_gender_rows($snp, \@sample_calls);
            push @rows, $x_row;
            push @rows, $y_row;
        } else {
            my $data_row = WTSI::NPG::Genotyping::VCF::DataRow->new(
                calls => \@sample_calls,
                additional_info => "ORIGINAL_STRAND=".$snp->strand
            );
            push @rows, $data_row;
        }
    }
    my $vcf_dataset = WTSI::NPG::Genotyping::VCF::VCFDataSet->new
        (header => $header,
         data   => \@rows);
    return $vcf_dataset;
}

sub _build_reference {
  my ($self) = @_;
  my $snpset_refs = $self->snpset->references;
  my @ref_names;
  foreach my $ref (@{$snpset_refs}) {
    push @ref_names, $ref->name;
  }
  if (scalar @ref_names == 0) {
    $self->logcroak("No reference names found in SNPSet initarg. Need to specify a reference initarg?");
  }
  return join ',', @ref_names;
}

sub _gender_rows {
    # create X and Y DataRow objects for the given gender marker and calls
    # we may have:
    # - X call, Y no-call (female sample)
    # - X call, Y call (male sample)
    # - X no-call, Y no-call (unknown gender)
    my ($self, $snp, $calls) = @_;
    my (@x_calls, @y_calls);
    foreach my $call (@{$calls}) {
        # output two data rows, for X and Y respectively
        # each row requires a list of calls
        my ($x_call, $y_call) = @{$call->xy_call_pair()};
        push @x_calls, $x_call;
        push @y_calls, $y_call;
    }
    my $x_row = WTSI::NPG::Genotyping::VCF::DataRow->new(
        snp             => $snp->x_marker,
        calls           => \@x_calls,
        additional_info => "ORIGINAL_STRAND=".$snp->strand
    );
    my $y_row = WTSI::NPG::Genotyping::VCF::DataRow->new(
        snp             => $snp->y_marker,
        calls           => \@y_calls,
        additional_info => "ORIGINAL_STRAND=".$snp->strand
    );
    return ($x_row, $y_row);
}

sub _parse_assay_results {
    # convert the AssayResultSet into Call objects
    # return a hash of hashes, indexed by marker and sample name
    my ($self) = @_;
    my %calls;
    my %samples;
    my $controls = 0;
    foreach my $resultSet (@{$self->resultsets()}) {
        foreach my $ar (@{$resultSet->assay_results()}) {
            # foreach AssayResult (Sequenom or Fluidigm)
            # use 'npg' methods to get snp, sample, call in standard format
            if ($ar->is_empty()) { next; }
            my $assay_adr = $ar->assay_address();
            if ($ar->is_control()) {
                $self->info("Found control assay in position ".$assay_adr);
                $controls++;
                next;
            }
            my $sample_id = $ar->canonical_sample_id();
            $samples{$sample_id} = 1;
            my $snp_id = $ar->snp_assayed();
            my $snp = $self->snpset->named_snp($snp_id);
            my $call;
            my $qscore = $ar->qscore();
            # * Using $ar->qscore() in place of $qscore in the
            # constructor does not work!
            # * The $ar->qscore() is not correctly interpolated into the
            # argument list, and the constructor dies because the argument
            # list has an odd number of elements
            # * TODO try to reproduce this error in a simplified test case
            if (is_GenderMarker($snp)) {
                $call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
                    snp      => $snp,
                    qscore   => $qscore,
                    genotype => $ar->canonical_call()
                );
            } else {
                $call = WTSI::NPG::Genotyping::Call->new(
                    snp      => $snp,
                    qscore   => $qscore,
                    genotype => $ar->canonical_call()
                );
            }
            $calls{$snp_id}{$sample_id} = $call;
        }
    }
    if ($controls > 0) { 
        my $msg = "Found ".$controls." controls out of ".
            scalar(@{$self->resultsets()})." samples.";
        $self->info($msg);
    }
    my @sortedSamples = sort(keys(%samples));
    return (\%calls, \@sortedSamples);
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::AssayResultParser

=head1 DESCRIPTION

A class for conversion of AssayResultSet objects from the Fluidigm/Sequenom
genotyping platforms to VCF format.

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
