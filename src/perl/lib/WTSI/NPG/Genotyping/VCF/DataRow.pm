use utf8;

package WTSI::NPG::Genotyping::VCF::DataRow;

use Moose;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'qual'    =>
    (is       => 'ro',
     isa      => 'Int',
     default  => -1,
     documentation => 'Phred quality score for alternate reference allele; -1 if missing. Not to be confused with quality scores of the calls for each sample.'
 );

has 'filter'  =>
    (is       => 'ro',
     isa      => 'Str',
     default  => '.',
     documentation => 'Filter status; if missing, represent by "."');

has 'additional_info'  =>
    (is       => 'ro',
     isa      => 'Str',
     default  => '.',
     documentation => 'Miscellaneous information string for VCF output');

has 'calls'   =>
    (is       => 'ro',
     isa      => 'ArrayRef[WTSI::NPG::Genotyping::Call]',
     required => 1,
     documentation => 'Call objects to convert to VCF row format');

# attributes derived from the list of input Genotype::Call objects

has 'snp' =>
    (is       => 'ro',
     isa      => Variant,
     lazy     => 1,
     builder  => '_build_snp');

has 'vcf_chromosome_name' =>
    (is       => 'ro',
     isa      => 'Str',
     lazy     => 1,
     builder => '_build_vcf_chromosome_name',
     documentation => 'Chromosome name string; may be 1-22, X, Y');

has 'is_haploid' =>
    (is       => 'ro',
     isa      => 'Bool',
     builder  => '_build_haploid_status',
     lazy     => 1,
     documentation => 'True for human Y chromosome, false otherwise'
    );

# NB this class does not have the sample names; they are stored in VCF header

# genotype sub-fields GT = genotype; GQ = genotype quality; DP = read depth
our $GENOTYPE_FORMAT = 'GT:GQ:DP';
our $DEPTH_PLACEHOLDER = 1;
our $DEFAULT_QUALITY_STRING = '.';
our $NULL_ALLELE = 'N';

sub BUILD {
    my ($self, ) = @_;
    if (scalar @{$self->calls} == 0) {
        $self->logcroak("Must input at least one Call to create a VCF row");
    }
}


=head2 to_string

  Arg [1]    : None

  Example    : $data_row->to_string
  Description: Return a string for output in the body of a VCF file.
  Returntype : Str

=cut

sub to_string {
    my ($self,)= @_;
    my @fields = ();
    my $alt;
    if ($self->is_haploid) { $alt = '.'; }
    else { $alt = $self->snp->alt_allele; }
    my $qual;
    if ($self->qual == -1) { $qual = '.'; }
    else {$qual = $self->qual; }
    push(@fields, ($self->vcf_chromosome_name,
                   $self->snp->position,
                   $self->snp->name,
                   $self->snp->ref_allele,
                   $alt,
                   $qual,
                   $self->filter,
                   $self->additional_info,
                   $GENOTYPE_FORMAT));
    foreach my $call (@{$self->calls}) {
        push(@fields, $self->_call_to_vcf_field($call));
    }
    return join("\t", @fields);
}

sub _build_haploid_status {
    # set haploid status to true or false
    # for now, the only haploid variants supported are human Y chromosome
    my ($self) = @_;
    if (is_YMarker($self->snp)) { return 1; }
    else { return 0; }
}

sub _build_snp {
    # find SNP from input calls; check that SNP name is consistent
    my ($self) = @_;
    my $snp;
    foreach my $call (@{$self->calls}) {
        if (!defined($snp)) {
            $snp = $call->snp;
        } elsif ($call->snp->name ne $snp->name) {
            $self->logcroak("Inconsistent SNP names for input to ",
                            "VCF::DataRow: '", $snp->name, "', '",
                            $call->snp->name, "'");
        }
    }
    return $snp;
}

sub _build_vcf_chromosome_name {
    # find a vcf-compatible chromosome name string
    my ($self) = @_;
    my $chr = $self->snp->chromosome;
    if ($chr =~ /^Chr/) {
        $chr =~ s/Chr//; # strip off 'Chr' prefix, if any
    }
    unless (is_HsapiensChromosomeVCF($chr)) {
        $self->logcroak("Unknown chromosome string: '",
                        $self->snp->chromosome, "'");
    }
    return $chr;
}

sub _call_to_vcf_field {
    # convert Call object to string of colon-separated sub-fields
    # sub-fields are genotype in VCf format, quality score, read depth
    my ($self, $call) = @_;
    if ($self->snp->strand eq '-') { # reverse strand, use complement of call
        $call = $call->complement();
    }
    my @alleles = split(//, $call->genotype);
    my $allele_total;
    if ($self->is_haploid()) { $allele_total = 1; }
    else { $allele_total = 2; }
    my $i = 0;
    my @vcf_alleles;
    while ($i < $allele_total) {
        my $allele = $alleles[$i];
        if ($allele eq $self->snp->ref_allele) { push(@vcf_alleles, '0'); }
        elsif ($allele eq $self->snp->alt_allele) { push(@vcf_alleles, '1'); }
        elsif ($allele eq $NULL_ALLELE) { push(@vcf_alleles, '.'); }
        $i++;
    }
    my $vcf_call = join('/', @vcf_alleles);
    my $qual;
    if (defined($call->qscore)) {
        if ($call->qscore == -1) {
            $qual = $DEFAULT_QUALITY_STRING;
        } else {
            $qual = $call->qscore;
        }
    } else {
        $qual = $DEFAULT_QUALITY_STRING;
    }
    my @subfields = ($vcf_call, $qual, $DEPTH_PLACEHOLDER);
    return join(':', @subfields);
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::DataRow

=head1 DESCRIPTION

Class to represent one row in the main body of a VCF file. Contains
data for a particular variant (eg. a SNP or gender marker), including
genotype calls for one or more samples.

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
