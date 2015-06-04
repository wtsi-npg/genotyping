
use utf8;

package WTSI::NPG::Genotyping::VCF::VCFConverter;

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

with 'WTSI::DNAP::Utilities::Loggable';

our $NULL_GENOTYPE = 'NN';
our $X_CHROM_NAME = 'X';
our $Y_CHROM_NAME = 'Y';
our @COLUMN_HEADS = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;

has 'chromosome_lengths' => ( # must be compatible with given snpset
    is           => 'ro',
    isa          => 'HashRef',
    required     => 1,
);

has 'snpset' => (
    is           => 'ro',
    isa          => 'WTSI::NPG::Genotyping::SNPSet',
    required => 1,
);

has 'resultsets' =>
    (is       => 'rw',
     isa      => 'ArrayRef', # Array of Sequenom OR Fluigidm AssayResultSet
    );

has 'sort' => ( # sort the sample names before output?
    is        => 'ro',
    isa       => 'Bool',
    default   => 1,
    documentation => 'If true, output rows are sorted in (chromosome, position) order',
    );

=head2 convert

  Arg [1]    : Path for VCF output (optional)

  Example    : $vcf_string = $converter->convert('/home/foo/output.vcf')
  Description: Convert the sample results stored in $self->resultsets to a
               single VCF format file. If the output argument is equal to '-',
               VCF will be printed to STDOUT; if a different output argument
               is given, it is used as the path for an output file; if the
               output argument is omitted, output will not be written. Return
               value is a string containing the VCF output.
  Returntype : Str

=cut

sub convert {
    my ($self, $output) = @_;
    my @out_lines = $self->_generate_vcf_complete();
    if ($self->sort) {
        @out_lines = $self->_sort_output_lines(\@out_lines);
    }
    my $out;
    my $outString = join("\n", @out_lines)."\n";
    if ($output) {
        $self->logger->info("Printing VCF output to $output");
        if ($output eq '-') {
            $out = *STDOUT;
        } else {
            open $out, '>:encoding(utf8)', $output ||
                $self->logcroak("Cannot open output '$output'");
        }
        print $out $outString;
        if ($output ne '-') {
            close $out || $self->logcroak("Cannot close output '$output'");
        }
    }
    return $outString;
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

sub _generate_vcf_complete {
    # generate VCF data given a SNPSet and one or more AssayResultSets
    my ($self, @args) = @_;
    my ($calls, $samples) = $self->_parse_assay_results();
    my @output; # lines of text for output
    my $header = WTSI::NPG::Genotyping::VCF::Header->new (
        sample_names       => $samples,
        chromosome_lengths => $self->chromosome_lengths,
    );
    push(@output, $header->to_string());
    foreach my $snp (@{$self->snpset->snps}) {
        push @output, $self->_generate_vcf_records($snp, $calls, $samples);
    }
    return @output;
}

sub _generate_vcf_records {
    my ($self, $snp, $calls, $samples) = @_;
    my @records;
    my @sample_calls;
    foreach my $sample (@$samples) {
        push(@sample_calls, $calls->{$snp->name}{$sample});
    }
    if (is_GenderMarker($snp)) {
        my (@x_calls, @y_calls);
        foreach my $call (@sample_calls) {
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
        push(@records, $x_row->to_string());
        push(@records, $y_row->to_string());
    } else {
        my $data_row = WTSI::NPG::Genotyping::VCF::DataRow->new(
            calls => \@sample_calls,
            additional_info => "ORIGINAL_STRAND=".$snp->strand
        );
        push(@records, $data_row->to_string());
    }
    return @records;
}

sub _parse_assay_results {
    # convert the AssayResultSet into Call objects
    # return a hash of hashes, indexed by marker and sample name
    my ($self, ) = @_;
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
            if (is_GenderMarker($snp)) {
                $call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
                    snp      => $snp,
                    qscore   => $ar->quality_score(),
                    genotype => $ar->canonical_call()
                );
            } else {
                $call = WTSI::NPG::Genotyping::Call->new(
                    snp      => $snp,
                    qscore   => $ar->quality_score(),
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

sub _read_json {
    # read given path into a string and decode as JSON
    my ($self, $input) = @_;
    open my $in, '<:encoding(utf8)', $input || 
        $self->logcroak("Cannot open input '$input'");
    my $data = decode_json(join("", <$in>));
    close $in || $self->logcroak("Cannot close input '$input'");
    return $data;
}

sub _sort_output_lines {
    # sort output lines by chromosome & position (1st, 2nd fields)
    # header lines are unchanged
    my ($self, $inputRef) = @_;
    my @input = @{$inputRef};
    my (@output, %chrom, %pos, @data);
    foreach my $line (@input) {
        if ($line =~ /^#/) {
            push @output, $line;
        } else {
            push(@data, $line);
            my @fields = split(/\s+/, $line);
            my $chr = shift(@fields);
            if ($chr eq $X_CHROM_NAME) { $chr = 23; }
            elsif ($chr eq $Y_CHROM_NAME) { $chr = 24; }
            $chrom{$line} = $chr;
            $pos{$line} = shift(@fields);
        }
    }
    @data = sort { $chrom{$a} <=> $chrom{$b} || $pos{$a} <=> $pos{$b} } @data;
    push @output, @data;
    return @output;
}

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

Copyright (c) 2014-2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
