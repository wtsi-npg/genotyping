use utf8;

package WTSI::NPG::Genotyping::VCF::VCFParser;

use Moose;

use Fcntl qw /:seek/;
use File::Temp qw /tempdir/;
use MooseX::Types::Moose qw(Int);
use Text::CSV;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::Types qw(:all);
use WTSI::NPG::Genotyping::VCF::DataRow;
use WTSI::NPG::Genotyping::VCF::Header;
use WTSI::NPG::Genotyping::VCF::VCFDataSet;

with 'WTSI::DNAP::Utilities::Loggable';

# inputs

has 'input_path'  =>
   (is            => 'ro',
    isa           => 'Str',
    required      => 1,
    documentation => 'Input path in iRODS or local filesystem',
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

has 'csv'         =>
   (is            => 'ro',
    isa           => 'Text::CSV',
    default       => sub { return Text::CSV->new({sep_char => "\t"}); },
    documentation => 'Object to parse tab-delimited input lines'
   );

# attributes built from inputs

has 'header'      =>
   (is            => 'ro',
    isa           => 'WTSI::NPG::Genotyping::VCF::Header',
    lazy          => 1,
    builder       => '_build_header',
   );

has 'input_file'  =>
   (is            => 'ro',
    isa           => 'FileHandle',
    lazy          => 1,
    builder       => '_build_input_file',
    documentation => 'Filehandle for input of VCF data',
   );


our $VERSION = '';
our $CHROM_INDEX = 0;
our $POS_INDEX = 1;
our $SNP_NAME_INDEX = 2;
our $REF_INDEX = 3;
our $ALT_INDEX = 4;
our $QSCORE_INDEX = 5;
our $FILTER_INDEX = 6;
our $INFO_INDEX = 7;
our $SAMPLE_START_INDEX = 9;

# methods to parse data row / header strings
# attribute for path to a snpset for creating call objects?
# have a get_next_datarow method to read a file in piecemeal

sub BUILD {
    # want initial position ready to read the first data row after header
    my ($self) = @_;
    my $status = $self->_seek_data_start();
    if (!$status) {
        $self->logcroak("Unable to seek to start of VCF data body");
    }
}

=head2 get_next_data_row

  Arg [1]    : None

  Example    : $row = parser->get_next_data_row
  Description: Get the next data row from the VCF file, or return undef if
               EOF has been reached.
  Returntype : Maybe[WTSI::NPG::Genotyping::VCF::DataRow]

=cut

sub get_next_data_row {
    my ($self) = @_;
    my $dataRow;
    my $line = readline $self->input_file;
    if ($line) {
        $dataRow = $self->_parse_data_row($line);
    }
    return $dataRow;
}

=head2 to_vcf_dataset

  Arg [1]    : None

  Example    : $dataset = $parser->to_vcf_dataset()
  Description: Convenience method to generate a VCFDataSet object. This
               reads the entire (parsed) contents of the VCF file into
               memory, which may not be desirable for large files.
  Returntype : WTSI::NPG::Genotyping::VCF::VCFDataSet

=cut

sub to_vcf_dataset {
    my ($self) = @_;
    my @dataRows;
    my $reading = 1;
    while ($reading) {
        my $dataRow = $self->get_next_data_row();
	if ($dataRow) {
	  push @dataRows, $dataRow;
	} else {
	  $reading = 0;
	}
    }
    my $dataset = WTSI::NPG::Genotyping::VCF::VCFDataSet->new
        (header => $self->header,
         data   => \@dataRows,
        );
    return $dataset;
}

sub _build_header {
    # read file position, seek to beginning of file, parse header,
    # and return to original position
    # need to find sample names and contig lengths
    my ($self) = @_;
    my $start_pos = tell $self->input_file;
    seek $self->input_file, 0, SEEK_SET;
    my $in_header = 1;
    # code to parse contig strings in in Header.pm instead of VCFParser.pm
    # (ensures definition of contig string format is all in the same class)
    # so, read the contig lines as strings and supply as alternative argument
    # to Header constructor
    my (@contig_lines, $sample_names);
    while ($in_header) {
        my $line = readline $self->input_file;
        if (eof $self->input_file) {
            $self->logcroak("Unexpected EOF while reading header");
        } elsif ($line =~ /^[#]{2}contig/msx ) {
            push @contig_lines, $line;
        } elsif ($line =~ /^[#]CHROM/msx ) {
            $sample_names = $self->_parse_sample_names($line);
        } elsif ($line !~ /^[#]/msx) {
            $in_header = 0;
        }
    }
    seek $self->input_file, $start_pos, SEEK_SET;
    return WTSI::NPG::Genotyping::VCF::Header->new
        (sample_names   => $sample_names,
         contig_strings => \@contig_lines);
}

sub _build_input_file {
    my ($self) = @_;
    my $localInputPath;
    if ($self->irods) {
      my $tmpdir = tempdir('vcf_parser_irods_XXXXXX', CLEANUP => 1);
      $localInputPath = "$tmpdir/input.vcf";
      $self->irods->get_object($self->input_path, $localInputPath);
    } else {
        $localInputPath = $self->input_path;
    }
    open my $file, "<", $localInputPath ||
        $self->logcroak("Cannot open input path '",
			$localInputPath, "'");
    return $file;
}

sub _parse_data_row {
    # parse a line from the main body of a VCF file and create a DataRow
    # required parameters:
    # - qscore of alternate reference allele, if any ('.' otherwise)
    # - filter status ('.' if missing)
    # - additional_info string
    # - ArrayRef of Call objects
    my ($self, $line) = @_;
    my @fields = $self->_split_tab_delimited_string($line);
    my $qscore_raw = $fields[$QSCORE_INDEX];
    my $qscore; # integer qscore if one is defined, undef otherwise
    if (is_Int($qscore_raw)) { $qscore = $qscore_raw; }
    my $snp_name = $fields[$SNP_NAME_INDEX];
    my $snp = $self->snpset->named_snp($snp_name);
    # TODO: do (chromosome, position) match in manifest and VCF file?
    # Problem: Different chromosome naming conventions, eg. Chr1 vs. 1
    my @calls;
    my $i = $SAMPLE_START_INDEX;
    while ($i < scalar @fields) {
      push(@calls, $self->_call_from_vcf_string(
          $fields[$i],
          $fields[$REF_INDEX],
          $fields[$ALT_INDEX],
          $snp)
       );
      $i++;
    }
    return WTSI::NPG::Genotyping::VCF::DataRow->new
      (qscore          => $qscore,
       filter          => $fields[$FILTER_INDEX],
       additional_info => $fields[$INFO_INDEX],
       calls           => \@calls);
}


sub _parse_sample_names {
    # parse sample names from the #CHROM line of a VCF file
    my ($self, $line) = @_;
    my @fields = $self->_split_tab_delimited_string($line);
    my $i = $SAMPLE_START_INDEX;
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

sub _call_from_vcf_string {
    # raw call of the form GT:GQ:DP
    # (Other VCF call formats not supported!)
    # GT = genotype
    # GQ = genotype quality (. if absent)
    # DP = read depth
    # arguments:
    # - VCF string
    # - $ref, alt alleles from vcf
    # - WTSI:Genotyping:SNP
    # return a WTSI:Genotyping:Call object
    my ($self, $input, $ref, $alt, $snp) = @_;
    my @terms = split /:/msx, $input;
    if (scalar @terms != 3) {
        $self->logcroak("Need exactly 3 colon-separated ",
                        "terms in call string");
    }
    my ($gt, $gq, $dp) = @terms;
    my ($genotype, $is_call);
    if ($gt eq '.') {
        $genotype = 'NN';
        $is_call = 0;
    } else {
        my $pattern = '[/]|[|]';
        my @gt = split qr/$pattern/msx, $gt;
        if (scalar @gt != 2) {
            $self->logcroak("Found (", join(', ', @gt),
                            "); expected exactly two alleles separated ",
                            "by '/' or '|'");
        }
        # members of @gt are one of (0, 1, .)
        if ($gt[0] eq '.' && $gt[1] eq '.') {
            $genotype = 'NN';
            $is_call = 0;
        } else {
            my @gt_alleles;
            if ($gt[0] eq '0') { push @gt_alleles, $ref; }
            elsif ($gt[0] eq '1') { push @gt_alleles, $alt; }
            else { $self->logcroak("Illegal allele code: '", $gt[0], "'"); }
            if ($gt[1] eq '0') { push @gt_alleles, $ref; }
            elsif ($gt[1] eq '1') { push @gt_alleles, $alt; }
            elsif ($gt[1] eq '.') { push @gt_alleles, $gt[0]; } # homozygote
            else { $self->logcroak("Illegal allele code: '", $gt[0], "'"); }
            $genotype = join '', @gt_alleles;
            $is_call = 1;
        }
    }
    my $qscore;
    if (is_Int($gq)) { $qscore = $gq; }
    return WTSI::NPG::Genotyping::Call->new
        (snp      => $snp,
         genotype => $genotype,
         is_call  => $is_call,
         qscore   => $qscore
     );
}

sub _seek_data_start {
    my ($self) = @_;
    # Position the filehandle ready to read the first row of data
    # after the header. Returns True if start successfully found,
    # False otherwise.
    my $success = 0;
    # do not check return value of seek
    # seek returns false if filehandle is already at target position
    seek $self->input_file, 0, SEEK_SET;
    while (!$success) {
        my $line = readline $self->input_file;
	if ($line =~ /^[#]CHROM/msx ) { $success = 1; }
    }
    return $success;
}

sub _split_tab_delimited_string {
    # use the CSV attribute to parse a tab delimited string
    # returns an array of strings for the tab delimited fields
    # removes newline (if any) from end of the string before splitting
    my ($self, $string) = @_;
    chomp $string;
    $self->csv->parse($string);
    return $self->csv->fields();
}


no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::VCFParser

=head1 DESCRIPTION

Class to read a VCF file and parse into Header and DataRow objects,
which can be used to create a VCFDataSet object.

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
