
# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2014

#
# Copyright (c) 2014 Genome Research Ltd. All rights reserved.
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

# Module for identity QC check against Sequenom calls

package WTSI::NPG::Genotyping::QC::Identity;

use warnings;
use strict;

use Carp;
use Cwd;
use JSON; # for testing only
use List::Util qw(max);
use POSIX qw(ceil);
use plink_binary; # in /software/varinf/gftools/lib ; front-end for C library
use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::QC::SnpID qw(illuminaToSequenomSNP);

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/run_identity_check/;

our %OUTPUT_NAMES = ('genotypes'  => 'identity_check_gt.txt',
		     'results'    => 'identity_check_results.txt',
		     'fail_pairs' => 'identity_check_failed_pairs.txt',
		     'json'       => 'identity_check.json'
    );
our $PLEX_DIR = '/nfs/srpipe_references/genotypes';
our $PLEX_FILE = 'W30467_snp_set_info_1000Genomes.tsv'; # W30467 has same snp set for 1000Genomes and GRCh37
our $log = Log::Log4perl->get_logger('genotyping.qc.identity');

# Check identity of Plink calls with a QC plex.
# QC plex method is currently Sequenom, may later extend to Fluidigm.
#
# Cross-references Plink with the QC plex to find number of available SNPs. If available SNPs are too few, omit the identity check.
# TODO include convenience method to cross-reference .bpm.csv manifest with QCplex.
# Compute match rate of (Plink calls, plex calls) for each sample. No-calls on any given sample are counted as mismatches.
# Samples with low match rate fail the QC metric.
# Compare calls on failed sample pairs to detect possible sample swaps.
# Write JSON file with identity metric for each sample, and supplementary text files.

sub compareFailedPairs {
    # pairwise check of all failed samples; use output to detect swaps
    # Consider sample pair (i, j)
    # let s_ij = rate of matching calls between (Illumina_i, Sequenom_j)
    # we may have s_ij != s_ji, so define pairwise metric as max(s_ij, s_ji)
    my %genotypes = %{ shift() };
    my @failedSamples = @{ shift() };
    my @snps = @{ shift() };
    my @comparison = ();
    for (my $i = 0; $i < @failedSamples; $i++) {
        for (my $j = 0; $j < $i; $j++) {
            my $sample_i = $failedSamples[$i];
            my $sample_j = $failedSamples[$j];
	    my @match = (0,0);
	    foreach my $snp (@snps) {
		my ($plink_i, $plex_i) = @{$genotypes{$sample_i}{$snp}};
		my ($plink_j, $plex_j) = @{$genotypes{$sample_j}{$snp}};
		my $equiv_ij = eval { equivalent($plink_i, $plex_j) };
		my $equiv_ji = eval { equivalent($plink_j, $plex_i) };
		if ($equiv_ij) { $match[0]++; }
		if ($equiv_ji) { $match[1]++; }
	    }
	    my $similarity = max(@match)/@snps;
	    push(@comparison, [$sample_i, $sample_j, $similarity]);
	}
    }
    return \@comparison;
}

sub equivalent {
    # check if given genotypes are equivalent to within:
    # - swap (major/minor allele reversal) and/or a flip (reverse complement)
    my ($gt0, $gt1) = @_;
    # basic sanity checking on input
    my $inputOK = 1;
    foreach my $gt ($gt0, $gt1) {
        if (length($gt)!=2) { $inputOK = 0; }
        elsif ($gt =~ /[^ACGT]/) { $inputOK = 0; }
    }
    unless ($inputOK) { croak "Incorrect arguments to equivalentGenotype: $gt0 $gt1\n"; }
    my $gt1Swap = join('', reverse(split('', $gt1))); # swap alleles
    if ($gt0 eq $gt1 || $gt0 eq $gt1Swap || $gt0 eq revComp($gt1) || 
        $gt0 eq revComp($gt1Swap) ) {
        return 1; # match
    } else {
        return 0; # no match
    }
}

sub findIdentity {
    # find the identity metric for each sample
    # return: metric values, genotypes by SNP & sample, pass/fail status
    my %plink = %{ shift() };
    my %sequenom = %{ shift() };
    my @snps = @{ shift() };
    my $minIdent = shift;
    my (%identity, %genotypes, %failed);
    foreach my $sample (keys(%plink)) {
	my $match = 0;
	foreach my $snp (@snps) {
	    my $pCall = $plink{$sample}{$snp};
	    my $sCall = $sequenom{$sample}{$snp};
	    if ($pCall && $sCall) {
		my $equiv = eval { equivalent($pCall, $sCall) };
		unless (defined($equiv)) {  
		    $log->logwarn("WARNING: ".$@); # error caught
		    $equiv = 0;
		}
		if ($equiv) { $match++; }
	    }
	    $pCall ||= 0;
	    $sCall ||= 0;
	    $genotypes{$sample}{$snp} = [$pCall, $sCall];
	}
	my $id = $match / @snps;
	$identity{$sample} = $id;
	if ($id < $minIdent) { $failed{$sample} = 1; }
    }
    return (\%identity, \%genotypes, \%failed);
}


sub getIntersectingSNPs {
    # find SNPs in Plink data which are also in QC plex
    # TODO modify to get plex file from IRODS
    my $pb = shift;
    my $plexPath = "$PLEX_DIR/$PLEX_FILE";
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($plexPath);
    my %sequenomSNPs;
    foreach my $name ($snpset->snp_names) { $sequenomSNPs{$name} = 1; } 
    # find Plink SNP names and cross-reference with Sequenom
    my @plinkSNPs;
    for my $i (0..$pb->{"snps"}->size() - 1) {
	my $name = $pb->{"snps"}->get($i)->{"name"};
	push @plinkSNPs, $name;
    }
    my @shared;
    foreach my $name (@plinkSNPs) {
	my $sqName = illuminaToSequenomSNP($name);
	if ($sequenomSNPs{$sqName}) { push(@shared, $sqName); }
    }
    return @shared;
}

sub getSampleNamesIDs {  
    # extract sample IDs from a plink_binary object
    # first, try parsing sampleName in standard PLATE_WELL_ID format
    # if unsuccessful, set sample ID = sampleName
    # output hash of IDs indexed by name
    # also get list of names (use to ensure consistent name order)
    my ($pb) = @_;  # $pb = plink_binary 
    my (%samples, @sampleNames);
    for my $i (0..$pb->{"individuals"}->size() - 1) {
        my $longName = $pb->{"individuals"}->get($i)->{"name"};
        my ($plate, $well, $id) = split /_/, $longName, 3;
        if ($id) {
            $samples{$longName} = $id;
        } else {
            $samples{$longName} = $longName;
        }
        push(@sampleNames, $longName);
    }
    return (\%samples, \@sampleNames);
}

sub getSequenomSNPNames {
    # read definitive Sequenom plex from iRODS, using SNPSet module
    # 2014-03-07 iRODS is having issues, use filename instead
    my $irods = WTSI::NPG::iRODS->new;
    my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$PLEX_DIR/$PLEX_FILE");
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);
    return $snpset->snp_names;
}

sub readPlinkCalls {
    # read genotype calls by sample & snp from given plink_binary object
    # requires list of sample names in same order as in plink file
    # return hash of calls by sample and SNP name
    my ($pb, $sampleNamesRef, $snpsRef) = @_;
    my @sampleNames = @$sampleNamesRef;
    my @snps = @$snpsRef;
    my %snps;
    foreach my $snp_id (@snps) { $snps{$snp_id} = 1; }
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    my %plinkCalls;
    while ($pb->next_snp($snp, $genotypes)) {
        # read calls from Plink binary object
        # try both "plink" and "sequenom" SNP name formats
        my $snp_id_illumina = $snp->{"name"};
        my $snp_id_sequenom = illuminaToSequenomSNP($snp_id_illumina);
        foreach my $snp_id ($snp_id_illumina, $snp_id_sequenom) {
            if (!$snps{$snp_id}) { next; }
            for my $i (0..$genotypes->size() - 1) {
                my $call = $genotypes->get($i);
                if ($call =~ /[N]{2}/) { next; } # skip 'NN' calls
                $plinkCalls{$sampleNames[$i]}{$snp_id} = $call;
            }
        }
    }
    return \%plinkCalls;
}

sub revComp {
    # reverse complement a DNA sequence
    my $seq = shift;
    my @bases = reverse(split('', $seq));
    my @rev = ();
    foreach my $base (@bases) {
	if ($base eq 'A') {push(@rev, 'T');}
	elsif ($base eq 'C') {push(@rev, 'G');}
	elsif ($base eq 'G') {push(@rev, 'C');}
	elsif ($base eq 'T') {push(@rev, 'A');}
	else {push(@rev, 'N'); }
    }
    return join('', @rev);
}

sub writeFailedPairComparison {
    my @compareResults = @{ shift() };
    my $maxSimilarity = shift;
    my $outDir = shift;
    my $outPath = $outDir.'/'.$OUTPUT_NAMES{'fail_pairs'};
    open my $out, ">", $outPath || $log->logcroak("Cannot open '$outPath'");
    my $header = join("\t", "#Sample_1", "Sample_2", "Similarity", "Status");
    print $out $header."\n";
    foreach my $resultRef (@compareResults) {
	my ($sample1, $sample2, $metric) = @$resultRef;
	my $status;
	if ($metric > $maxSimilarity) { $status = 'SWAP_WARNING'; }
	else { $status = 'NO_MATCH'; }
	my $line = sprintf("%s\t%s\t%.4f\t%s\n", $sample1, $sample2, $metric, $status);
	print $out $line;
    }
    close $out || $log->logcroak("Cannot close '$outPath'");
}

sub writeGenotypes {
    my %genotypes = %{ shift() }; # hashes of calls by sample & snp
    my @snps = @{ shift() }; # list of SNPs to output
    my $outDir = shift;
    my @samples = sort(keys(%genotypes));
    open my $gt, ">", $outDir.'/'.$OUTPUT_NAMES{'genotypes'} or die $!;
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
    my %identity = %{ shift() }; # hash of identity by sample
    my %failed = %{ shift() };   # pass/fail status by sample
    my %missing = %{ shift() };  # missing samples from Sequenom query 
    my @samples = @{ shift() };  # list ensures consistent sample name order
    my $snpTotal = shift;
    my $minIdent = shift;
    my $outDir = shift;
    open my $results, ">",  $outDir.'/'.$OUTPUT_NAMES{'results'} or die $!;
    my $header = join("\t", "# Identity comparison",
		      "MIN_IDENTITY:$minIdent", 
                      "AVAILABLE_PLEX_SNPS:$snpTotal")."\n";
    $header .= join("\t", "# sample", "concordance", "result")."\n";
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

sub writeJson {
    # get data structure for output to and write to JSON file
    # first argument is hash of values (if check was run) or list of samples (if check was not run)
    my ($resultsRef, $idCheck, $minSnps, $commonSnps, $outDir) = @_;
    my $idRef;
    my %data = (results => $resultsRef,
		identity_check_run => $idCheck,
		min_snps => $minSnps,
		common_snps => $commonSnps
	);
    my $outPath = $outDir.'/'.$OUTPUT_NAMES{'json'};
    open my $out, ">", $outPath || $log->logcroak("Cannot open '$outPath'");
    print $out encode_json(\%data);
    close $out || $log->logcroak("Cannot close '$outPath'");
}

sub run_identity_check {
    # 'main' method to run identity check
    my ($plinkPrefix, $outDir, $minCheckedSNPs, $minIdent, $swap, $iniPath, $warn) = @_;
    my $pb = new plink_binary::plink_binary($plinkPrefix);
    $pb->{"missing_genotype"} = "N"; 

    # 1) Read sample names and IDs from Plink
    my ($samplesRef, $sampleNamesRef) = getSampleNamesIDs($pb);
    $log->debug("Sample names read from PLINK binary.\n"); 
    my @snps = getIntersectingSNPs($pb); # definitive list of SNPs for metric
    my $snpTotal = @snps;
    if ($snpTotal < $minCheckedSNPs) {
	my %id;
	foreach my $sample (@{$sampleNamesRef}) { $id{$sample} = 'NA'; }
	writeJson(\%id, 0, $minCheckedSNPs, $snpTotal, $outDir);
	my $msg = "Cannot do identity check; $minCheckedSNPs SNPs from QC ".
	    "plex required, $snpTotal found";
	if ($warn) { $log->logwarn($msg); }
    } else {
	# 2) Read Sequenom results from SNP DB
	my $snpdb = WTSI::NPG::Genotyping::Database::SNP->new
	    (name   => 'snp',
	     inifile => $iniPath)->connect(RaiseError => 1);
	my ($sqnmCallsRef, $sqnmSnpsRef, $missingSamplesRef, $sqnmTotal) 
	    = $snpdb->find_sequenom_calls_by_sample($samplesRef);
	$log->debug($sqnmTotal." calls read from Sequenom.\n"); 
	
	# 3) Read PLINK genotypes for all samples; can take a while!
	my $start = time();
	my $plinkCallsRef = readPlinkCalls($pb, $sampleNamesRef, \@snps);
	#print to_json($plinkCallsRef, { pretty => 1 })."\n";
	my $duration = time() - $start;
	$log->debug("Calls read from PLINK binary: $duration seconds.\n"); 
	
	# 4) Find identity, genotypes, and pass/fail status; write output
	my ($idRef, $gtRef, $failRef) = findIdentity($plinkCallsRef, $sqnmCallsRef, \@snps, $minIdent);
	writeJson($idRef, 1, $minCheckedSNPs, $snpTotal, $outDir);
	writeGenotypes($gtRef, \@snps, $outDir);
	writeIdentity($idRef, $failRef, $missingSamplesRef, $sampleNamesRef,
		      $snpTotal, $minIdent, $outDir);
	
	# 5) Pairwise check on failed samples for possible swaps
	my @failed = sort(keys(%{$failRef}));
	my $compareRef = compareFailedPairs($gtRef, \@failed, \@snps, $swap);
	writeFailedPairComparison($compareRef, $minIdent, $outDir);
	
	$log->debug("Finished identity check.\n");
    }

    return 1;
}

1;
