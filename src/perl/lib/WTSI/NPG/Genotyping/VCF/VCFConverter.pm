
use utf8;

package WTSI::NPG::Genotyping::VCF::VCFConverter;

use DateTime;
use JSON;
use List::AllUtils qw(uniq);
use Log::Log4perl::Level;
use Moose;

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResultSet;
use WTSI::NPG::Genotyping::Types qw(:all);
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

with 'WTSI::DNAP::Utilities::Loggable';

our $SEQUENOM_TYPE = 'sequenom';
our $FLUIDIGM_TYPE = 'fluidigm';
our $CHROMOSOME_JSON_KEY = 'chromosome_json';
our $DEFAULT_READ_DEPTH = 1;
our $DEFAULT_QUALITY = 40;
our $NULL_ALLELE = 'N';
our $X_CHROM_NAME = 'X';
our $Y_CHROM_NAME = 'Y';
our @COLUMN_HEADS = qw/CHROM POS ID REF ALT QUAL FILTER INFO FORMAT/;

has 'chromosome_lengths' => ( # must be compatible with given snpset
    is           => 'ro',
    isa          => 'HashRef',
    required     => 1,
);

has 'irods'   =>
    (is       => 'ro',
     isa      => 'WTSI::NPG::iRODS',
     required => 1,
     default  => sub {
     return WTSI::NPG::iRODS->new;
     });

has 'input_type' => (
    is           => 'ro',
    isa          => 'Str',
    default      => 'sequenom', # sequenom or fluidigm
);

has 'snpset' => ( # pipeline SNPSet object
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
    );

has 'normalize_chromosome' => ( # normalize the chromosome name to GRCh37?
    is        => 'ro',
    isa       => 'Bool',
    default   => 1,
);

sub BUILD {
  my $self = shift;
  # Make our iRODS handle use our logger by default
  $self->irods->logger($self->logger);
  my @results;
  my $input_type = $self->input_type;
  if ($input_type ne $SEQUENOM_TYPE && $input_type ne $FLUIDIGM_TYPE) {
      $self->logcroak("Unknown input data type: '$input_type'");
  }
}


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

sub _call_to_vcf {
    # input a 'npg' call from an AssayResult (Sequenom or Fluidigm)
    # convert to VCF representation
    my ($self, $call, $ref, $alt, $strand) = @_;
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
    my (@new_alleles, $new_call);
    my @alleles = split(//, $call);
    my $alleles_ok = 1;
    foreach my $allele (@alleles) {
        if ($reverse) { $allele = $complement{$allele}; }
        if ($allele eq $ref) { push(@new_alleles, '0'); }
        elsif ($allele eq $alt) { push(@new_alleles, '1'); }
        elsif ($allele eq $NULL_ALLELE) { push(@new_alleles, '.'); }
        elsif ($ref eq $alt && $allele ne $ref) { $alleles_ok = 0; last; }
        else { $self->logcroak("Non-null call '$allele' does not match ",
                               "reference '$ref' or alternate '$alt'");  }
    }
    if ($alleles_ok) { $new_call = join('/', @new_alleles); }
    else { $new_call = ''; } # special case; failed gender marker
    return $new_call;
}

sub _generate_vcf_complete {
    # generate VCF data given a SNPSet and one or more AssayResultSets
    my ($self, @args) = @_;
    my ($callsRef, $samplesRef) = $self->_parse_calls_samples();
    my %calls = %{$callsRef};

    my @output; # lines of text for output
    my @samples = sort(keys(%{$samplesRef}));
    my ($chroms, $snpset);
    $snpset = $self->snpset;
    my $total = scalar(@{$snpset->snps()});
    push(@output, $self->_generate_vcf_header(\@samples));

    foreach my $snp (@{$snpset->snps}) {
        if (is_GenderMarker($snp)) {
          push @output,
            $self->_generate_vcf_records($snp->x_marker, \%calls, \@samples),
            $self->_generate_vcf_records($snp->y_marker, \%calls, \@samples);
        }
        else {
            push @output,
              $self->_generate_vcf_records($snp, \%calls, \@samples);
        }
    }

    return @output;
}

sub _generate_vcf_records {
    my ($self, $snp, $calls, $samples) = @_;

    my $read_depth = $DEFAULT_READ_DEPTH; # placeholder
    my $qscore = $DEFAULT_QUALITY;        # placeholder genotype quality

    my $ref = $snp->ref_allele();
    my $alt = $snp->alt_allele();
    my $chrom = $snp->chromosome();
    if ($self->normalize_chromosome) {
        $chrom = $self->_normalize_chromosome_name($chrom);
    }

    my @fields = ( $chrom,                    # CHROM
                   $snp->position(),          # POS
                   $snp->name(),              # ID
                   $ref,                      # REF
                   $alt,                      # ALT
                   '.', '.',                  # QUAL, FILTER
                   'ORIGINAL_STRAND='.$snp->strand(),  # INFO
                   'GT:GQ:DP',                # FORMAT
                 );

    my @records;
    foreach my $sample (@$samples) {
        my $call_raw = $calls->{$snp->name}{$sample};
        my $call = $self->_call_to_vcf($call_raw, $ref, $alt, $snp->strand());
        $call_raw ||= ".";
        $call ||= ".";
        my @sample_fields = ($call, $qscore, $read_depth);
        push(@fields, join(':', @sample_fields));
    }

    push(@records, join("\t", @fields));

    return @records;
}

sub _generate_vcf_header {
    my ($self, $samplesRef) = @_;
    my %lengths = %{$self->chromosome_lengths};
    my @samples = @{$samplesRef};
    my $dt = DateTime->now(time_zone=>'local');
    my @header = ();
    push(@header, '##fileformat=VCFv4.0');
    push(@header, '##fileDate='.$dt->ymd(''));
    push(@header, '##source=WTSI_NPG_genotyping_pipeline');
    # add contig tags with chromosome lengths to prevent bcftools warnings
    my @chromosomes = sort(keys(%lengths));
    foreach my $chr (@chromosomes) {
        my $line = "##contig=<ID=$chr,length=$lengths{$chr},".
            "species=\"Homo sapiens\">";
        push(@header, $line);
    }
    my @lines = (
        '##INFO=<ID=ORIGINAL_STRAND,Number=1,Type=String,'.
            'Description="Direction of strand in input file">',
        '##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">',
        '##FORMAT=<ID=GQ,Number=1,Type=Integer,'.
            'Description="Genotype Quality">',
        '##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read Depth">',
    );
    foreach my $line (@lines) { push(@header, $line); }
    my @colHeads = @COLUMN_HEADS;
    push(@colHeads, @samples);
    push(@header, "#".join("\t", @colHeads));
    return @header;
}

sub _normalize_chromosome_name {
    # convert the chromosome field to standard GRCh37 format
    # chromsome names: 1, 2, 3, ... , 22, X, Y
    my ($self, $input) = @_;
    my $output;
    if ($input =~ /^[0-9]+$/ && $input >= 1 && $input <= 22 ) {
        $output = $input; # already in numeric chromosome format
    } elsif ($input eq 'X' || $input eq 'Y') {
        $output = $input; # already in standard X/Y format
    } elsif ($input =~ /^Chr/) {
        $input =~ s/Chr//g; # strip off 'Chr' prefix
        $output = $self->_normalize_chromosome_name($input);
    } else {
        $self->logcroak("Unknown chromosome string: \"$input\"");
    }
    return $output;
}

sub _parse_calls_samples {
    # parse calls and sample IDs from reference to an array of ResultSets
    #  use 'npg' methods to get snp, sample, call in standard format
    # for either Fluidigm or Sequenom
    my ($self) = @_;
    my (%calls, %samples);
    # generate a hash of calls by SNP and sample, and list of sample IDs
    my $controls = 0;
    foreach my $resultSet (@{$self->resultsets()}) {
        foreach my $ar (@{$resultSet->assay_results()}) {
            my $assay_pos = $ar->assay_position();
            if ($ar->is_control()) {
                $self->info("Found control assay in position ".$assay_pos);
                $controls++;
                next;
            }
            my $sam_id = $ar->npg_sample_id();
            unless ($sam_id) {
                $self->logwarn("Missing sample ID for assay ".$assay_pos);
                next;
            }
            my $snp_id = $ar->snp_assayed();
            unless ($snp_id) {
                # missing SNP ID is normal for control position
                my ($sample, $assay_num) = $ar->parse_assay();
                my $msg = "Missing SNP ID for sample '$sam_id', ".
                    "assay '$assay_pos'";
                $self->logwarn($msg);
                next;
            }
            my $call = $ar->npg_call();
            my $previous_call = $calls{$snp_id}{$sam_id};
            if ($previous_call && $previous_call ne $call) {
                my $msg = 'Conflicting genotype calls for SNP '.$snp_id.
                    ' sample '.$sam_id.': '.$call.', '.$previous_call;
                $self->logwarn($msg);
                $call = '';
            }
            $calls{$snp_id}{$sam_id} = $call;
            $samples{$sam_id} = 1;
        }
    }
    if ($controls > 0) { 
        my $msg = "Found ".$controls." controls out of ".
            scalar(@{$self->resultsets()})." samples.";
        $self->info($msg);
    }
    return (\%calls, \%samples);
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

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
