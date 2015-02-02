#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Cwd;
use Getopt::Long;
use JSON;

# convert JSON output from check_identity_bed.pl to legacy text files

my ($input, $summary, $failures, $genotypes, $help);

run() unless caller();

sub run {

    GetOptions("in=s"         => \$input,
               "summary=s"    => \$dbPath,
               "failures=s"   => \$configPath,
               "genotypes=s"  => \$iniPath,
               "h|help"       => \$help);



}

# subroutines copy-pased from old Identity.pm
# TODO edit to read input from JSON and create appropriate text files

sub writeFailedPairComparison {
    my $self = shift;
    my @compareResults = @{ shift() };
    my $maxSimilarity = shift;
    my $outPath = $self->output_dir.'/'.$self->output_names->{'fail_pairs'};
    open my $out, ">", $outPath || 
        $self->logger->logcroak("Cannot open '$outPath'");
    my $header = join("\t", "#Sample_1", "Sample_2", "Similarity", "Swap_warning");
    print $out $header."\n";
    foreach my $resultRef (@compareResults) {
	my ($sample1, $sample2, $metric) = @$resultRef;
	my $status;
	if ($metric > $maxSimilarity) { $status = 'TRUE'; }
	else { $status = 'FALSE'; }
	my $line = sprintf("%s\t%s\t%.4f\t%s\n", $sample1, $sample2, $metric, $status);
	print $out $line;
    }
    close $out || $self->logger->logcroak("Cannot close '$outPath'");
}

sub writeGenotypes {
    my $self = shift;
    my %genotypes = %{ shift() }; # hashes of calls by sample & snp
    my @snps = @{ shift() }; # list of SNPs to output
    my @samples = sort(keys(%genotypes));
    my $outPath = $self->output_dir.'/'.$self->output_names->{'genotypes'};
    open my $gt, ">", $outPath or die $!;
    my @heads = qw/SNP sample illumina_call qc_plex_call/;
    print $gt '#'.join("\t", @heads)."\n";
    foreach my $snp (@snps) {
	foreach my $sample (sort(keys(%genotypes))) {
	    my ($pCall, $sCall) = @{ $genotypes{$sample}{$snp} };
	    $pCall ||= '-';
	    $sCall ||= '-';
	    print $gt join("\t", $snp, $sample, $pCall, $sCall), "\n";
	}
    }
    close $gt or die $!;
}

sub writeIdentity {
    # evaluate identity pass/fail and write results
    # return list of failed sample names
    my $self = shift;
    my %identity = %{ shift() }; # hash of identity by sample
    my %failed = %{ shift() };   # pass/fail status by sample
    my %missing = %{ shift() };  # missing samples from Sequenom query
    my @samples = @{ shift() };  # list ensures consistent sample name order
    my $snpTotal = shift;
    my $minIdent = shift;
    my $outPath = $self->output_dir.'/'.$self->output_names->{'results'};
    open my $results, ">", $outPath or die $!;
    my $header = join("\t", "#Identity comparison",
		      "MIN_IDENTITY:$minIdent", 
                      "AVAILABLE_PLEX_SNPS:$snpTotal")."\n";
    $header .= join("\t", "#sample", "concordance", "result")."\n";
    print $results $header;
    foreach my $sample (@samples) {
	my $line;
	if (!($missing{$sample})) {
	    $line = sprintf("%s\t%.4f\t", $sample, $identity{$sample});
	    if ($failed{$sample}) { $line .= "Fail\n"; }
	    else { $line .= "Pass\n"; }
	} else {
	    $line = join("\t", $sample, "-", "Unavailable")."\n";
	}
	print $results $line;
    }
    close $results;
}


__END__

=head1 NAME

WTSI::NPG::Genotyping::QC::Identity

=head1 DESCRIPTION

Script to convert JSON output from check_identity_bed.pl to old-style
text files. Outputs are tab-delimited text and include a summary, list of
failed samples, and list of all genotype calls used for identity.

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
