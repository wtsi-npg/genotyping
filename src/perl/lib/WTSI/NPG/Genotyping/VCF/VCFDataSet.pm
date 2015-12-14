use utf8;

package WTSI::NPG::Genotyping::VCF::VCFDataSet;

use Moose;
use Set::Scalar;
use WTSI::NPG::Genotyping::GenderMarkerCall;
use WTSI::NPG::Genotyping::VCF::DataRow;
use WTSI::NPG::Genotyping::VCF::Header;

with 'WTSI::DNAP::Utilities::Loggable';

has 'header'  =>
    (is       => 'ro',
     isa      => 'WTSI::NPG::Genotyping::VCF::Header',
     required => 1,
    );

has 'data'    =>
    (is       => 'ro',
     isa      => 'ArrayRef[WTSI::NPG::Genotyping::VCF::DataRow]',
     required => 1,
    );

has 'num_samples' =>
    (is           => 'ro',
     isa          => 'Int',
     lazy         => 1,
     builder      => '_build_num_samples',
    );

has 'sort_output' =>
   (is        => 'ro',
    isa       => 'Bool',
    default   => 1,
    documentation => 'If true, output rows are sorted in (chromosome, position) order',
    );

has 'merge_x_y_markers' =>
    (is       => 'ro',
     isa      => 'Bool',
     default  => 1,
     documentation => 'If true, merge pairs of calls on (X,Y) chromosomes with the same variant name into single GenderMarker calls.',
 );

our $VERSION = '';

our $X_CHROM_NAME = 'X';
our $Y_CHROM_NAME = 'Y';

sub BUILD {
    my ($self) = @_;
    # forces num_samples to be evaluated, including consistency checks
    # safer than having a non-lazy attribute
    my $msg = "Created VCF dataset for ".$self->num_samples." samples.";
    $self->info($msg);
}

=head2 calls_by_sample

  Arg [1]    : None
  Example    : my $calls_hashref = $vcf_data_set->calls_by_sample()
  Description: Return a hashref containing Call objects for each sample.
               Transposes the variant-major format of VCF to sample-major.
  Returntype : HashRef[ArrayRef[WTSI::NPG::Genotyping::Call]]

=cut

sub calls_by_sample {
    my ($self) = @_;
    my %calls_by_sample;
    my @sample_names = @{$self->header->sample_names};
    my @calls = @{$self->_find_calls()};
    for (my $i=0;$i<@calls;$i++) {
        for (my $j=0;$j<@sample_names;$j++) {
            my $call = $calls[$i][$j];
            if (defined($call)) {
                push @{$calls_by_sample{$sample_names[$j]}}, $call;
            } else {
                $self->logconfess("SNP call undefined for ", $i, "th SNP, ",
                                  $j, "th sample (indexed from zero)");
            }
        }
    }
    return \%calls_by_sample;
}

=head2 str

  Arg [1]    : None
  Example    : my $vcf_string = $vcf_data_set->str()
  Description: Return a string which can be output as a VCF file.
  Returntype : Str

=cut

sub str {
    my ($self) = @_;
    my @output;
    foreach my $row (@{$self->data}) {
        push @output, $row->str();
    }
    if ($self->sort_output) {
        @output = $self->_sort_output_lines(\@output);
    }
    # prepend header to output
    unshift @output, $self->header->str();
    return join "\n", @output;
}

=head2 write_vcf

  Arg [1]    : Output path, or '-' for STDOUT
  Example    : $vcf_data_set->write_vcf($output_path);
  Description: Write the dataset in VCF format to the given path, or
               STDOUT if the path is a dash, '-'.
  Returntype : Int

=cut

sub write_vcf {
    # convert to string and write to the path (or - for STDOUT)
    my ($self, $output) = @_;
    my $outString = $self->str();
    if ($output) {
        my $out;
        $self->logger->info("Printing VCF output to $output");
        if ($output eq '-') {
            $out = *STDOUT;
        } else {
            open $out, '>:encoding(utf8)', $output ||
                $self->logcroak("Cannot open output '$output'");
        }
        print $out $outString."\n";
        if ($output ne '-') {
            close $out || $self->logcroak("Cannot close output '$output'");
        }
    }
}

sub _build_num_samples {
    my ($self) = @_;
    my $num_samples = scalar @{$self->header->sample_names};
    foreach my $row (@{$self->data}) {
        my $num_calls = scalar @{$row->calls};
        if ($num_calls != $num_samples) {
            $self->logcroak("Inconsistent number of samples for ",
                            "VCF dataset: Header has ", $num_samples,
                            " variant '", $row->snp->name, "' has ",
                            $num_calls);
        }
    }
    return $num_samples;
}


sub _find_calls {
    # find an array of arrays of calls from the component DataRow objects
    # if necessary, merge DataRows for X and Y markers into GenderMarker calls
    my ($self) = @_;
    my @all_calls;
    my %x_row_calls;
    my %y_row_calls;
    foreach my $row (@{$self->data}) {
        my $is_x = $row->snp->is_XMarker;
        my $is_y = $row->snp->is_YMarker;
        my $key = $row->snp->name;
        if ($self->merge_x_y_markers && ($is_x || $is_y)) {
            if ($is_x) {
                if (defined($x_row_calls{$key})) {
                    $self->logconfess("Multiple X markers named ", $key, "'");
                }
                $x_row_calls{$key} = $row->calls;
            }
            if ($is_y) {
                if (defined($y_row_calls{$key})) {
                    $self->logconfess("Multiple Y markers named ", $key, "'");
                }
                $y_row_calls{$key} = $row->calls;
            }
        } else {
            push @all_calls, $row->calls;
        }
    }
    # now pair up X/Y calls to create GenderMarkerCalls
    my $x_name_set = Set::Scalar->new(keys %x_row_calls);
    my $y_name_set = Set::Scalar->new(keys %y_row_calls);
    unless ($x_name_set->is_equal($y_name_set)) {
        $self->logconfess("Cannot complete X/Y pairs; orphan names from ",
                          "X markers [", join(', ', keys(%x_row_calls)),
                          "], Y markers [", join(', ', keys(%y_row_calls)),
                          "]");
    }
    foreach my $key (keys %x_row_calls) { # for each variant name
        my @merged_calls;
        my @x_calls = @{$x_row_calls{$key}};
        my @y_calls = @{$y_row_calls{$key}};
        my $total = scalar @x_calls;
        unless ($total == scalar @y_calls) {
            $self->logconfess("Cannot merge X/Y marker call ",
                              "lists of different lengths");
        }
        for (my $i=0;$i<$total;$i++) {
            my $csn_x = $x_calls[$i]->callset_name;
            my $csn_y = $y_calls[$i]->callset_name;
            my $call;
            if ($csn_x eq $csn_y) {
                $call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
                    x_call       => $x_calls[$i],
                    y_call       => $y_calls[$i],
                    callset_name => $csn_x
                );
            } else {
                $self->logwarn("Conflicting callset names ",
                               "for X and Y calls: [", $csn_x, ", ", $csn_y,
                               "]. Using ",
                               "default name for GenderMarkerCall");
                $call = WTSI::NPG::Genotyping::GenderMarkerCall->new(
                    x_call       => $x_calls[$i],
                    y_call       => $y_calls[$i]
                );
            }
            push @merged_calls, $call;
        }
        push @all_calls, \@merged_calls;
    }
    return \@all_calls;
}

sub _sort_output_lines {
    # sort output lines by chromosome & position (1st, 2nd fields)
    # header lines are unchanged
    my ($self, $inputRef) = @_;
    my @input = @{$inputRef};
    my (@output, %chrom, %pos, @data);
    foreach my $line (@input) {
        if ($line =~ m/^[#]/msx) {
            push @output, $line;
        } else {
            push @data, $line;
            my @fields = split /\s+/msx, $line ;
            my $chr = shift @fields;
            if ($chr eq $X_CHROM_NAME) { $chr = 23; }
            elsif ($chr eq $Y_CHROM_NAME) { $chr = 24; }
            $chrom{$line} = $chr;
            $pos{$line} = shift @fields;
        }
    }
    @data = sort { $chrom{$a} <=> $chrom{$b} || $pos{$a} <=> $pos{$b} } @data;
    push @output, @data;
    return @output;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::VCFDataSet

=head1 DESCRIPTION

A complete data set in Variant Call Format, including a header and one or
more data rows.

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
