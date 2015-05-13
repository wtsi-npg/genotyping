use utf8;

package WTSI::NPG::Genotyping::VCF::DataRow;

use Moose;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Types qw(:all);

with 'WTSI::DNAP::Utilities::Loggable';

has 'qual'    => # Phred quality score for alternate allele; -1 if missing
    (is       => 'ro',
     isa      => 'Int',
     default  => -1);

has 'filter'  => # filter status; if missing, represent by '.'
    (is       => 'ro',
     isa      => 'Str',
     default  => '.');

has 'additional_info'  => # miscellaneous information string
    (is       => 'ro',
     isa      => 'Str',
     default  => '.');

has 'calls'   => # Genotype::ScoredCall objects; convert to VCF format
    (is       => 'ro',
     isa      => 'ArrayRef[WTSI::NPG::Genotyping::Call]');


# attributes derived from the list of input Genotype::Call objects

has 'snp' =>
    (is       => 'rw',
     isa      => Variant);

has 'vcf_chromosome' => # chromosome; may be 1-22, X, Y
    (is       => 'rw',
     isa      => 'Str');

# genotype sub-fields GT = genotype; GQ = genotype quality; DP = read depth
# TODO are GQ and DP required by bcftools?
our $GENOTYPE_FORMAT = 'GT:GQ:DP';
our $DEPTH_PLACEHOLDER = 1;
our $DEFAULT_QUALITY_STRING = '.';

our $NULL_ALLELE = 'N';

# NB this class does not have the sample names; they are stored in VCF header

sub BUILD {

    my ($self, ) = @_;
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
    $self->snp($snp);
    $self->vcf_chromosome($self->_chromosome_name_to_vcf($snp->chromosome));
    my $total_calls = scalar(@{$self->calls});
}

sub to_string {
    my ($self,)= @_;
    my @fields = ();
    my $qual;
    if ($self->qual == -1) { $qual = '.'; }
    else {$qual = $self->qual; }
    push(@fields, ($self->vcf_chromosome,
                   $self->snp->position,
                   $self->snp->name,
                   $self->snp->ref_allele,
                   $self->snp->alt_allele,
                   $qual,
                   $self->filter,
                   $self->additional_info,
                   $GENOTYPE_FORMAT));
    foreach my $call (@{$self->calls}) {
        push(@fields, $self->_call_to_vcf_field($call));
    }
    return join("\t", @fields);
}

sub _call_to_vcf_field {
    # cf. _call_to_vcf in VCFConverter
    my ($self, $call) = @_;
    my $ref = $self->snp->ref_allele;
    my $alt = $self->snp->alt_allele;
    my $strand = $self->snp->strand;
    if (!defined($call) || !$call) {
        return './.';
    }
    my %complement = ('A' => 'T',
                      'C' => 'G',
                      'G' => 'C',
                      'T' => 'A',
                      'N' => 'N');
    my $reverse;
    if ($strand eq '+') { $reverse = 0; }
    elsif ($strand eq '-') { $reverse = 1; }
    else { $self->logcroak("Unknown strand value '$strand'"); }
    my (@vcf_alleles, $vcf_call);
    my @alleles = split(//, $call->genotype);
    my $alleles_ok = 1;
    foreach my $allele (@alleles) {
        if ($reverse) { $allele = $complement{$allele}; }
        if ($allele eq $ref) { push(@vcf_alleles, '0'); }
        elsif ($allele eq $alt) { push(@vcf_alleles, '1'); }
        elsif ($allele eq $NULL_ALLELE) { push(@vcf_alleles, '.'); }
        elsif ($ref eq $alt && $allele ne $ref) { $alleles_ok = 0; last; }
        else { $self->logcroak("Non-null call '$allele' does not match ",
                               "reference '$ref' or alternate '$alt'");  }
    }
    if ($alleles_ok) { $vcf_call = join('/', @vcf_alleles); }
    else { $vcf_call = ''; } # special case; failed gender marker
    # construct a VCF genotype field
    # sub-fields are call, quality score, read depth
    # TODO are read depth and a non-null quality score required?
    my $qual;
    if ($call->qscore == -1) { $qual = $DEFAULT_QUALITY_STRING; }
    else { $qual = $call->qscore; }
    my @subfields = ($vcf_call, $qual, $DEPTH_PLACEHOLDER);
    return join(':', @subfields);
}

sub _chromosome_name_to_vcf {
    # cf.  _normalize_chromosome_name in VCFConverter
    my ($self, $input) = @_;
    my $output;
    if ($input =~ /^[0-9]+$/ && $input >= 1 && $input <= 22 ) {
        $output = $input; # already in numeric chromosome format
    } elsif ($input eq 'X' || $input eq 'Y') {
        $output = $input; # already in standard X/Y format
    } elsif ($input =~ /^Chr/) {
        $input =~ s/Chr//g; # strip off 'Chr' prefix
        $output = $self->_chromosome_name_to_vcf($input);
    } else {
        $self->logcroak("Unknown chromosome string: \"$input\"");
    }
    return $output;
}


no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::Call

=head1 DESCRIPTION

Class to represent one row in the main body of a VCF file. Contains
data for a particular SNP, including one or more genotype calls.

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
