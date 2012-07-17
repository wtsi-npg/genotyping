#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# June 2012

#
# Copyright (c) 2012 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

# Module to read/write PLINK data
# Initially for gender check, may have other uses

use warnings;
use strict;
use Carp;
use plink_binary; # from gftools package
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/checkPlinkBinaryInputs/;

sub checkPlinkBinaryInputs {
    # check that PLINK binary files exist and are readable
    my $plinkPrefix = shift;
    my @suffixes = qw(.bed .bim .fam);
    my $inputsOK = 1;
    foreach my $suffix (@suffixes) {
	my $path = $plinkPrefix.$suffix;
	unless (-r $path) { $inputsOK = 0; last; } 
    }
    return $inputsOK;
}

sub countCallsHets {
    # filter SNPs on call rate and PAR location; count successful calls, and het calls, for SNPs passing filters
    my ($pb, $sampleNamesRef, $minCR, $log, $verbose, $includePar) = @_;
    my ($snpTotal, $snpFail, $snpPar) = (0,0,0);
    $includePar ||= 0; # remove SNPs from pseudoautosomal regions
    $log ||= 0;
    if ($log) { writeSnpLogHeader($log); }
    my (%allHets, %allCalls);
    my @sampleNames = @$sampleNamesRef;
    foreach my $name (@sampleNames) { $allHets{$name} = 0; }
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    while ($pb->next_snp($snp, $genotypes)) {
	my ($callsRef, $hetsRef, $cr) = snpCallsHets($snp, $genotypes, $sampleNamesRef);
	my $crPass;
	if ($cr >= $minCR) { $crPass = 1; }
	else { $crPass = 0; }
	my $par = isXPAR($snp);
	if ($log) { updateLog($log, $snp, $cr, $crPass, $par); }
	if (!$crPass) { $snpFail++; next; }
	elsif (!$includePar && $par) { $snpPar++; next; }
	$snpTotal++;
	my %calls = %$callsRef;
	my %hets = %$hetsRef;
	foreach my $sample (keys(%calls)) { $allCalls{$sample}++; }
	foreach my $sample (keys(%hets)) { $allHets{$sample}++; }
    }
    if ($snpTotal==0) { croak "ERROR: No valid SNPs found: $!"; } 
    elsif ($verbose) { print "$snpTotal SNPs passed, $snpPar from PARs rejected, $snpFail failed CR check\n"; }
    return (\%allCalls, \%allHets, $snpTotal);   
}


sub extractChromData {
    # write plink binary data to given directory, for given chromosome only (defaults to X)
    # return plink_binary object for reading data
    my ($plinkPrefix, $outDir, $chrom, $outPrefix, $verbose) = @_;
    $chrom ||= "X";
    $outPrefix ||= "plink_temp_chrom$chrom";
    $verbose ||= 0;
    my $outArg = $outDir."/$outPrefix";
    my $cmd = "/software/bin/plink --bfile $plinkPrefix --chr $chrom --out $outArg --make-bed";
    unless ($verbose) { $cmd .= " > /dev/null"; } # suppress stdout from plink
    system($cmd);
    my $pb = new plink_binary::plink_binary($outArg);
    return $pb;
}

sub findHetRates {
    # find het rates by sample for given plink binary and sample names
    # het rate defined as: successful calls / het calls for each sample, on snps satisfying min call rate
    my ($pb, $sampleNamesRef, $log, $includePar, $minCR, $verbose) = @_;
    $includePar = 0;
    $minCR ||= 0.95;
    $log ||= 0;
    $verbose ||= 0;
    my @sampleNames = @$sampleNamesRef;
    if ($verbose) { print STDERR "Finding SNP evaluation set.\n"; }
    my ($allCallsRef, $allHetsRef, $snps) = countCallsHets($pb, $sampleNamesRef,$minCR,$log,$verbose,$includePar);
    if ($verbose) { print STDERR $snps." SNPs found for het rate computation.\n"; }
    my %allCalls = %$allCallsRef;
    my %allHets = %$allHetsRef;
    my %hetRates = ();
    for my $i (0..$#sampleNames) {
	my $name = $sampleNames[$i];
	my $hetRate;
	if ($allCalls{$name} > 0) { $hetRate = $allHets{$name} / $allCalls{$name}; }
	else { $hetRate = 0; }
	$hetRates{$name} = $hetRate;
	if ($verbose && $i % 100 == 0) {print $i." ".$name." ".$hetRate."\n"; }
    }
    return %hetRates;
}

sub getSampleNamesGenders {
    # get sample names and genders from given plink binary object
    # (may want to compare inferred and supplied genders)
    my $pb = shift;
    my (@sampleNames, @sampleGenders);
    for my $i (0..$pb->{"individuals"}->size() - 1) {
	my $name = $pb->{"individuals"}->get($i)->{"name"};
	my $gender = $pb->{"individuals"}->get($i)->{"sex"};
	push(@sampleNames, $name);
	push(@sampleGenders, $gender);
    }
    return (\@sampleNames, \@sampleGenders);
}

sub isXPAR {
    # does given SNP (plink_binary object) on x chromosome fall into pseudoautosomal region?
    my $snp = shift;
    my $chrom = $snp->{"chromosome"};
    if ($chrom ne 'X' && $chrom!=23) { 
	croak "Non-X SNP supplied to X chromosome PAR check: Chromosome $snp->{\"chromosome\"}: $!"; 
    }
    my @xPars = ( # X chrom pseudoautosomal regions; from NCBI GRCh37 Patch Release 8 (GRCh37.p8), 2012-04-12
	[60001,	2699520],
	[154931044, 155260560],
	);
    my $snp_pos = $snp->{"physical_position"};
    my $par = 0;
    foreach my $pairRef (@xPars) {
	my ($start, $end) = @$pairRef;
	if ($snp_pos>=$start && $snp_pos <= $end) { $par = 1; last; }
    }
    return $par;
}

sub snpCallsHets {
    # count calls/hets and find call rate for given (plink_binary) SNP & genotypes
    my ($snp, $genotypes, $samplesRef) = @_;
    my $noCalls = 0;
    my %hets = ();
    my %calls = ();
    my $total = $genotypes->size();
    my @sampleNames = @$samplesRef;
    for (my $i=0;$i<$total;$i++) { # calls for each sample on current snp
	my $call = $genotypes->get($i);
	if ($call =~ /[N]{2}/) { 
	    $noCalls++;
	} else {
	    $calls{$sampleNames[$i]} = 1;
	    if (substr($call, 0, 1) ne substr($call, 1, 1)) { $hets{$sampleNames[$i]} = 1; }
	}
    }
    my $cr;
    if ($total>0) { $cr = 1 - ($noCalls/$total); }
    else { $cr = undef; }
    return (\%calls, \%hets, $cr);
}

sub updateLog {
    my ($log, $snp, $cr, $par) = @_;
    my $snp_id = $snp->{"name"};
    my $snp_chrom = $snp->{"chromosome"};
    my $snp_pos = $snp->{"physical_position"};
    printf $log "%s\t%s\t%s\t%.6f\t%s\n", ($snp_id, $snp_chrom, $snp_pos, $cr, $par);
}

sub writeSnpLogHeader {
    my $log = shift;
    my @headers = qw(SNP chromosome position CR is_PAR);
    print $log join("\t", @headers)."\n";
}


return 1;
