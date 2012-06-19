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

use warnings;
use strict;
use plink_binary; # from gftools package

sub countCallsHets {
    # count successful calls, and het calls, for each snp in given plink binary
    my ($pb, $sampleNamesRef, $verbose, $minCR) = @_;
    my @sampleNames = @$sampleNamesRef;
    my %allHets = ();
    my %allCalls = ();
    foreach my $name (@sampleNames) { $allHets{$name} = 0; }
    my $snps = 0;
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    my $snpFail = 0;
    while ($pb->next_snp($snp, $genotypes)) {
	# get calls for each SNP; if call rate OK, update het count for each sample
	my $snp_id = $snp->{"name"};
	my $samples = 0;
	my $noCalls = 0;
	my %hets = ();
	my %calls = ();
	for my $i (0..$genotypes->size() - 1) { # calls for each sample on current snp
	    my $call = $genotypes->get($i);
	    $samples++;
	    if ($call =~ /[N]{2}/) { 
		$noCalls++;
	    } else {
		$calls{$sampleNames[$i]} = 1;
		if (substr($call, 0, 1) ne substr($call, 1, 1)) { $hets{$sampleNames[$i]} = 1; }
	    }
	}
	# update global call/het counts
	if (1-($noCalls/$samples) >= $minCR) {
	    foreach my $sample (keys(%calls)) { $allCalls{$sample}++; }
	    foreach my $sample (keys(%hets)) { $allHets{$sample}++; }
	    $snps++;
	    my $cr = 1 - $noCalls/$samples;
	    if ($verbose && $snps % 1000 == 0) { print $snps." ".$cr."\n"; }
	} else {
	    $snpFail++;
	}
    }
    if ($snps==0) { die "ERROR: No valid SNPs found: $!"; } 
    elsif ($verbose) { print "$snps SNPs passed, $snpFail failed\n"; }
    return (\%allCalls, \%allHets, $snps);
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
    my ($pb, $sampleNamesRef, $verbose, $minCR) = @_;
    my @sampleNames = @$sampleNamesRef;
    $minCR ||= 0.95;
    my ($allCallsRef, $allHetsRef, $snps) = countCallsHets($pb, $sampleNamesRef, $verbose, $minCR);
    if ($verbose) { print $snps." SNPs found for het rate computation.\n"; }
    my %allCalls = %$allCallsRef;
    my %allHets = %$allHetsRef;
    my %hetRates = ();
    for my $i (0..$#sampleNames) {
	my $name = $sampleNames[$i];
	my $hetRate;
	if ($allCalls{$name} > 0) { $hetRate = $allHets{$name} / $allCalls{$name}; }
	else { $hetRate = 0; }
	$hetRates{$name} = $hetRate;
	if ($verbose && $i % 100 == 0) {print $i." ".$sampleNames[$i]." ".$hetRate."\n"; }
    }
    return %hetRates;
}

sub getSampleNamesGenders {
    # get sample names and genders from given plink binary object
    # (may want to compare inferred and supplied genders)
    my $pb = shift;
    my $verbose = shift;
    my (@sampleNames, @sampleGenders);
    for my $i (0..$pb->{"individuals"}->size() - 1) {
	my $name = $pb->{"individuals"}->get($i)->{"name"};
	my $gender = $pb->{"individuals"}->get($i)->{"sex"};
	push(@sampleNames, $name);
	push(@sampleGenders, $gender);
	if ($verbose && $i % 100 == 0) {print $i." ".$name."\n"; }
    }
    return (\@sampleNames, \@sampleGenders);
}



return 1;
