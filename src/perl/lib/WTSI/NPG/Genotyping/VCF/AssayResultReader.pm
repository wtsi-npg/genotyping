
use utf8;

package WTSI::NPG::Genotyping::VCF::AssayResultReader;

use JSON;
use List::AllUtils qw(uniq);
use Log::Log4perl::Level;
use Moose;

use WTSI::NPG::iRODS;
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

our $NULL_GENOTYPE = 'NN';
our $SEQUENOM_TYPE = 'sequenom'; # TODO remove redundancy wrt vcf_from_plex.pl
our $FLUIDIGM_TYPE = 'fluidigm';
our @COLUMN_HEADS = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;

has 'contig_lengths' => # must be compatible with given snpset
   (is            => 'ro',
    isa           => 'HashRef[Int]',
    required      => 1,
    documentation => 'Used to generate contig tags required by bcftools. '.
                     'In typical usage, each contig corresponds to a '.
                     'homo sapiens chromosome.',
   );

has 'inputs'      =>
   (is            => 'ro',
    isa           => 'ArrayRef[Str]',
    required      => 1,
    documentation => 'Input paths in iRODS or local filesystem',
   );

has 'input_type'  =>
   (is            => 'ro',
    isa           => Platform,
    required      => 1,
   );

has 'irods'       =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::iRODS',
    documentation => '(Optional) iRODS instance from which to read data. '.
                     'If not given, inputs are assumed to be paths on the '.
                     'local filesystem.',
   );

has 'reference'   =>
   (is            => 'ro',
    isa           => 'Str',
    lazy          => 1,
    builder       => '_build_reference',
    documentation => 'String to populate the ##reference field in '.
                     'a VCF header. Defaults to a comma-separated '.
                     'list of reference names from the snpset argument.'
   );

has 'resultsets' =>
   (is       => 'ro',
    isa      => ArrayRefOfResultSet,
    lazy     => 1,
    builder  => '_build_resultsets',
   );

has 'snpset' =>
   (is           => 'ro',
    isa          => 'WTSI::NPG::Genotyping::SNPSet',
    required => 1,
   );

sub BUILD {
    my ($self) = @_;
    if (scalar @{$self->inputs} == 0) {
        $self->logcroak("Must have at least one input path",
                        " or iRODS location!");
    }
}

=head2 get_vcf_dataset

  Arg [1]    : None

  Example    : my $vcf_data = $reader->get_vcf_dataset();
  Description: Create a VCFDataSet object using the AssayResultSets which
               have been read.
  Returntype : WTSI::NPG::Genotyping::VCF::VCFDataSet

=cut

sub get_vcf_dataset {
    my ($self) = @_;
    my ($calls, $samples) = $self->_parse_assay_results();
    my $header = WTSI::NPG::Genotyping::VCF::Header->new(
        sample_names => $samples,
        contig_lengths => $self->contig_lengths,
	reference => $self->reference,
    );
    my @rows;
    foreach my $snp (@{$self->snpset->snps}) {
        my @sample_calls;
        foreach my $sample (@$samples) {
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

sub _build_resultsets {
    my ($self) = @_;
    my @results;
    if (defined($self->irods)) { # read input from iRODS
        foreach my $input (@{$self->inputs}) {
            my $resultSet;
            if ($self->input_type eq $SEQUENOM_TYPE) {
                my $data_obj =
                    WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new(
                        $self->irods, $input);
                $resultSet =
                    WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
                        data_object => $data_obj);
            } elsif ($self->input_type eq $FLUIDIGM_TYPE) {
                my $data_obj =
                    WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new(
                        $self->irods, $input);
                $resultSet =
                    WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
                        data_object => $data_obj);
            } else {
                $self->logcroak();
            }
            push @results, $resultSet;
        }
    } else { # read input from local filesystem
         foreach my $input (@{$self->inputs}) {
             my $resultSet;
             if ($self->input_type eq $SEQUENOM_TYPE) {
                 $resultSet =
                     WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
                         $input);
             } elsif ($self->input_type eq $FLUIDIGM_TYPE) {
                 $resultSet =
                     WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
                         $input);
             } else {
                 $self->logcroak();
             }
             push @results, $resultSet;
         }
    }
    return \@results;
}

sub _gender_rows {
    # create X and Y DataRow objects for the given gender marker and calls
    my ($self, $snp, $calls) = @_;
    my (@x_calls, @y_calls);
    foreach my $call (@{$calls}) {
        # output two data rows, for X and Y respectively
        # (at least) one of the two outputs is a no-call
        my $base = substr($call->genotype, 0, 1);
        if ($call->is_x_call()) {
            push(@x_calls, WTSI::NPG::Genotyping::Call->new(
                snp      => $snp->x_marker,
                genotype => $call->genotype,
                qscore   => $call->qscore,
            ));
            push(@y_calls, $self->_generate_no_call($snp->y_marker));
        } elsif ($call->is_y_call()) {
            push(@x_calls, $self->_generate_no_call($snp->x_marker));
            push(@y_calls, WTSI::NPG::Genotyping::Call->new(
                snp      => $snp->y_marker,
                genotype => $call->genotype(),
                qscore   => $call->qscore(),
            ));
        } elsif (!$call->is_call()) {
            push(@x_calls, $self->_generate_no_call($snp->x_marker));
            push(@y_calls, $self->_generate_no_call($snp->y_marker));
            push @x_calls, WTSI::NPG::Genotyping::Call->new(
                snp      => $snp->x_marker,
                genotype => $call->genotype,
                qscore   => $call->qscore,
            );
            push @y_calls, $self->_generate_no_call($snp->y_marker);
        } elsif ($call->is_y_call()) {
            push @x_calls, $self->_generate_no_call($snp->x_marker);
            push @y_calls, WTSI::NPG::Genotyping::Call->new(
                snp      => $snp->y_marker,
                genotype => $call->genotype(),
                qscore   => $call->qscore(),
            );
        } elsif (!$call->is_call()) {
            push @x_calls, $self->_generate_no_call($snp->x_marker);
            push @y_calls, $self->_generate_no_call($snp->y_marker);
        } else {
            $self->logcroak("Invalid genotype of '", $call->genotype,
                            "' for gender marker ", $snp->name,
                            "; valid non-null alleles are ",
                            $snp->ref_allele, " or ",
                            $snp->alt_allele);
        }
    }
    my $x_row = WTSI::NPG::Genotyping::VCF::DataRow->new(
        calls => \@x_calls,
        additional_info => "ORIGINAL_STRAND=".$snp->strand
    );
    my $y_row = WTSI::NPG::Genotyping::VCF::DataRow->new(
        calls => \@y_calls,
        additional_info => "ORIGINAL_STRAND=".$snp->strand
    );
    return ($x_row, $y_row);
}

sub _generate_no_call {
    # convenience method to generate a no-call on the given variant
    my ($self, $snp) = @_;
    return WTSI::NPG::Genotyping::Call->new(
        snp      => $snp,
        genotype => $NULL_GENOTYPE,
        is_call  => 0,
    );
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

WTSI::NPG::Genotyping::VCF::VCFConverter

=head1 DESCRIPTION

A class for conversion of output files from the Fluidigm/Sequenom genotyping
platforms to VCF format.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
