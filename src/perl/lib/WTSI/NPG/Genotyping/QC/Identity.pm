
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
use plink_binary; # in /software/varinf/gftools/lib ; front-end for C library
use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::QC::SnpID qw(illuminaToSequenomSNP);

use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/run_identity_check/;

our %OUTPUT_NAMES = ('genotypes'  => 'identity_check_gt.txt',
		     'results'    => 'identity_check_results.txt',
		     'fail'       => 'identity_check_fail.txt',
		     'fail_pairs' => 'identity_check_failed_pairs.txt',
		     'fail_match' => 'identity_check_failed_pairs_match.txt',
		     'log'        => 'identity_check.log',
    );

sub compareGenotypes {
    # read plink and (if available) sequenom genotypes by SNP and sample
    # write genotypes to file
    # also compare genotypes for equivalence and return results
    my (%sampleNames, %snpNames, @sampleNames, @snpNames, $plinkCall, 
        $sqnmCall, %count, %match);
    my %plinkCalls = %{ shift() }; # hashes of calls by sample & snp
    my %sqnmCalls =  %{ shift() };
    my $outDir = shift; # output path
    # generate lists of SNP and sample names
    foreach my $callsRef (\%plinkCalls, \%sqnmCalls) {
        my %calls = %$callsRef;
        foreach my $sample (keys(%calls)) {
            $sampleNames{$sample} = 1;
            foreach my $snp (keys(%{$calls{$sample}})) { $snpNames{$snp} = 1; }
        }
    }
    @sampleNames = keys(%sampleNames);
    @snpNames = keys(%snpNames);
    # sorting not strictly necessary, but ensures consistent output order
    @sampleNames = sort(@sampleNames); 
    @snpNames = sort(@snpNames);
    # open output file and print headers
    open my $gt, ">", $outDir.'/'.$OUTPUT_NAMES{'genotypes'} or die $!; 
    print $gt "#".join("\t", qw(SNP sample genotype sequenom))."\n";
    # write genotypes to file and populate comparison hash
    foreach my $snp (@snpNames) {
        foreach my $sample (@sampleNames) {
            my $plinkCall = $plinkCalls{$sample}{$snp};
            unless ($plinkCall) { $plinkCall = '-'; }
            my $sqnmCall = $sqnmCalls{$sample}{$snp};
            unless ($sqnmCall) { $sqnmCall = '-'; }
            print $gt join("\t", $snp, $sample, $plinkCall, $sqnmCall), "\n";
            # no comparison if one call is missing
            next if ($plinkCall eq '-' || $sqnmCall eq '-');
            $count{$sample}++;
            my $equiv = eval { 
                genotypesAreEquivalent($sqnmCalls{$sample}{$snp}, $plinkCall) 
            };
            if (!defined($equiv)) {  
                print STDERR "WARNING: ".$@; # error caught
                $equiv = 0;
            } 
            $match{$sample}++ if $equiv;
        }
    }
    close $gt or die $!;
    return (\%count, \%match);
}

sub compareFailedPairs {
    # do pairwise check of all failed samples
    # (in case sample IDs were swapped in Sequenom or Illumina)
    # for samples (i, j) compare SNP calls:  (Sequenom_i, Illumina_j)
    # NOTE: in general, (Sequenom_i, Illumina_j) != (Sequenom_j, Illumina_i) 
# on shared SNP subsets and concordance
    # So, test is not symmetric!  But for a real "failed pair", 
# would expect high similarity on (i,j) and (j,i).
    my %plinkCalls = %{ shift() };
    my %sqnmCalls = %{ shift() };
    my @failedSamples = @{ shift() };
    my (@count, @match);
    for (my $i = 0; $i < @failedSamples; $i++) {
        for (my $j = 0; $j < @failedSamples; $j++) {
            next if $i == $j; # (i,i) is guaranteed to match!
            my $sample_i = $failedSamples[$i];
            my $sample_j = $failedSamples[$j];
            foreach my $snp (keys %{$sqnmCalls{$sample_i}}) { 
                #start with Sequenom calls, compare to Illumina
                my $plinkCall = $plinkCalls{$sample_j}{$snp};
                next unless $plinkCall;
                $count[$i][$j] += 1;
                # ensure all counts have corresponding match entry
                unless ($match[$i][$j]) { $match[$i][$j] = 0; } 
                my $equiv = eval { 
                    genotypesAreEquivalent($sqnmCalls{$sample_i}{$snp}, 
                                           $plinkCall) 
                };
                unless (defined($equiv)) {  
                    print STDERR "WARNING: ".$@; # error caught
                    $equiv = 0;
                }
                $match[$i][$j] += 1 if $equiv;
            }
        }
    }
    return (\@count, \@match);
}

sub genotypesAreEquivalent {
    # check if given genotypes are equivalent
    # to within a swap (major/minor allele reversal) and/or a flip (reverse complement)
    my ($gt0, $gt1) = @_;
    # basic sanity checking on input
    my $inputOK = 1;
    foreach my $gt ($gt0, $gt1) {
        if (length($gt)!=2) { $inputOK = 0; }
        elsif ($gt =~ /[^ACGT]/) { $inputOK = 0; }
    }
    unless ($inputOK) { die "Incorrect arguments to equivalentGenotype: $gt0 $gt1\n"; }
    my $gt1Swap = join('', reverse(split('', $gt1))); # swap alleles
    if ($gt0 eq $gt1 || $gt0 eq $gt1Swap || $gt0 eq revComp($gt1) || 
        $gt0 eq revComp($gt1Swap) ) {
        return 1; # match
    } else {
        return 0; # no match
    }
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
    my $total = @sampleNames;
    return (\%samples, \@sampleNames, $total);
}

sub readPlinkCalls {
    # read genotype calls by sample & snp from given plink_binary object
    # requires list of sample names in same order as in plink file
    # return hash of calls by sample and SNP name
    my ($pb, $sampleNamesRef, $sqnmSnpsRef) = @_;
    my @sampleNames = @$sampleNamesRef;
    my %sqnmSnps = %$sqnmSnpsRef;
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    my %plinkCalls;
    my $start = time();
    while ($pb->next_snp($snp, $genotypes)) {
        # read SNPs from Plink binary object, look for Sequenom equivalents
        # try both "plink" and "sequenom" SNP name formats
        my $snp_id_illumina = $snp->{"name"};
        my $snp_id_sequenom = illuminaToSequenomSNP($snp_id_illumina);
        foreach my $snp_id ($snp_id_illumina, $snp_id_sequenom) {
            if (!$sqnmSnps{$snp_id}) { next; }
            for my $i (0..$genotypes->size() - 1) {
                my $call = $genotypes->get($i);
                if ($call =~ /[N]{2}/) { next; } # skip 'NN' calls
                $plinkCalls{$sampleNames[$i]}{$snp_id} = $call;
            }
        }
    }
    my $duration = time() - $start;
    return (\%plinkCalls, $duration);
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

sub writeComparisonResults {
    # write results of identity check to files
    # possible results: Skipped, Pass, Fail, Unavailable
    my %count = %{ shift() }; # count of valid genotype call pairs, by sample
    my %match = %{ shift() }; # count of matching pairs, by sample
    my %missing = %{ shift() }; # samples missing from Sequenom
    my $min_checked_snps = shift;
    my $min_ident = shift;
    my $outDir = shift;
    
    my %failedSamples;
    open my $results, ">",  $outDir.'/'.$OUTPUT_NAMES{'results'} or die $!;
    open my $fail, ">", $outDir.'/'.$OUTPUT_NAMES{'fail'} or die $!;
    my $header = join("\t", "# Sequenom identity comparison", 
                      "MIN_SNPS:$min_checked_snps", 
                      "PASS_THRESHOLD:$min_ident")."\n";
    $header .= join("\t", "# sample", "common SNPs", "matching calls", 
                    "concordance", "result")."\n";
    print $results $header;
    print $fail $header;
    # write skipped/pass/fail samples to RESULTS; fail samples to FAIL
    foreach my $sample (keys %count) {
        my $concord = $match{$sample} / $count{$sample};
        my $line = sprintf("%s\t%d\t%d\t%.4f\t",
                           $sample,
                           $count{$sample},
                           $match{$sample},
                           $concord);
        if ($count{$sample} < $min_checked_snps) {
            $line .= "Skipped\n";
        } elsif ($concord >= $min_ident) {
            $line .= "Pass\n";
        } else {
            $failedSamples{$sample} = 1;
            $line .= "Fail\n";
            print $fail $line;
        }
        print $results $line;
    }
    close $fail;
    # write missing samples
    foreach my $sample (keys %missing) {
        print $results join("\t", $sample, ".", ".", ".", "Unavailable"), "\n";
    }
    close $results;
    return %failedSamples; 
}

sub writeFailedPairCheck {
    # write results of check on failed sample pairs
    # summarise for all pairwise results; details for possible swaps
    my ($samplesRef, $countRef, $matchRef, $minIdent, $outDir) = @_;
    my $digits = 3; # precision for output
    my @matchedPairs = 
        writeFailedPairResults($samplesRef, $countRef, $matchRef, $minIdent, 
                               $outDir, $digits);
    writeFailedPairDetails(\@matchedPairs, $samplesRef, $countRef, $matchRef,
                           $outDir, $digits);
    return 1;
}

sub writeFailedPairDetails {
    my ($mPairsRef, $samplesRef, $countRef, $matchRef, $outDir, $digits) = @_;
    $digits ||= 3;
    my @matchedPairs = @$mPairsRef;
    my @samples = @$samplesRef;
    my @count = @$countRef;
    my @match = @$matchRef;
    my %wroteMatch;
    my $header = "# Details of sample pairs (S1, S2) which failed at ".
        "least one pairwise check\n";
    $header .= "# Check A:B finds Sequenom calls for B, then compares to ".
        "Illumina calls for A\n";
    $header .= join("\t", "# S1", "S2", "S1:S2_shared_SNPs", "S1:S2_matches", 
                    "S1:S2_concordance", "S2:S1_shared_SNPs", "S2:S1_matches", 
                    "S2:S1_concordance")."\n"; 
    open my $detail, ">", $outDir.'/'.$OUTPUT_NAMES{'fail_match'} or die $!;
    print $detail $header;
    foreach my $pairRef (@matchedPairs) {
        my ($i, $j) = @$pairRef; # $i = sequenom sample, $j = illumina sample
        # no need to write results for both (i,j) and (j,i)
        if ($wroteMatch{$samples[$j]}{$samples[$i]}) { next; }
        else { $wroteMatch{$samples[$i]}{$samples[$j]} = 1; }
        my @words = ($samples[$j], $samples[$i], # Illumina name goes first
                     $count[$i][$j], $match[$i][$j], 
                     sprintf("%.${digits}f", $match[$i][$j]/$count[$i][$j]), 
                     $count[$j][$i], $match[$j][$i], 
                     sprintf("%.${digits}f", $match[$j][$i]/$count[$j][$i]),  
            );
        print $detail join("\t", @words)."\n";
    }
    close $detail or die $!;    
}

sub writeFailedPairResults {
    # do all pairwise checks and write results to file
    # list failed pairs with SNP call match above $minIdent threshold
    my ($samplesRef, $countRef, $matchRef, $minIdent, $outDir, $digits) = @_;
    $digits ||= 3;
    my @samples = @$samplesRef;
    my @count = @$countRef;
    my @match = @$matchRef;
    my $header = "# Pairwise check for possible ID swaps, on all samples ".
        "which failed identity check.\n";
    $header .= "# Check A:B finds Sequenom calls for B and ".
        "compares to Illumina calls for A\n";
    $header .= "# MIN_IDENTITY_FOR_MATCH:$minIdent\n";
    $header .= join("\t", "Illumina", "Sequenom", "common_SNPs", 
                    "matching_calls", "concordance", "result")."\n";
    my @matchedPairs;
    open my $results, ">", $outDir.'/'.$OUTPUT_NAMES{'fail_pairs'} or die $!;
    print $results $header;
    for (my $i = 0; $i < @samples; $i++) {
        for (my $j = 0; $j < @samples; $j++) {
            next if $i == $j;
            my $status;
            if ($count[$i][$j]==0) { 
                next; 
			} elsif ($match[$i][$j] / $count[$i][$j] < $minIdent) { 
                $status = "NO_MATCH"; 
			} else { 
                $status = "SWAP_WARNING";
                push (@matchedPairs, [$i, $j]);
			}
            # print illumina sample name first, then sequenom
            my $metric = sprintf("%.${digits}f", $match[$i][$j]/$count[$i][$j]);
            print $results join("\t",  $samples[$j], $samples[$i], 
                                $count[$i][$j], $match[$i][$j], 
                                $metric, $status)."\n";
        }
    }
    close $results or die $!;
    return @matchedPairs;
}

# TODO cross-reference Sequenom plex with manifest to find number of available SNPs for identity check.
# Find Sequenom plex from IRODS (?)
# Compare with manifest -- requires a manifest argument
# Compute match rate for each SNP (vs. maximum)
# Count no-calls as mismatches? Or require minimum number of calls for check?


sub run_identity_check {
    # 'main' method to run identity check
    my ($plinkPrefix, $outDir, $minCheckedSNPs, $minIdent, $manifest, $iniPath) = @_;
    open my $logfile, ">", $outDir.'/'.$OUTPUT_NAMES{'log'} || die $!; 
    my $pb = new plink_binary::plink_binary($plinkPrefix);
    $pb->{"missing_genotype"} = "N"; 
    # get sample names and IDs from Plink file
    my ($samplesRef, $sampleNamesRef, $total) = getSampleNamesIDs($pb);
    print $logfile $total." samples read from PLINK binary.\n"; 
    # get Sequenom genotypes for all samples 
    my $snpdb = WTSI::NPG::Genotyping::Database::SNP->new
        (name   => 'snp',
         inifile => $iniPath)->connect(RaiseError => 1);
    my ($sqnmCallsRef, $sqnmSnpsRef, $missingSamplesRef, $sqnmTotal) 
        = $snpdb->find_sequenom_calls_by_sample($samplesRef);
    print $logfile $sqnmTotal." calls read from Sequenom.\n"; 
    # get PLINK genotypes for all samples; can take a while!
    my ($plinkCallsRef, $duration) 
        = readPlinkCalls($pb, $sampleNamesRef, $sqnmSnpsRef);
    print $logfile "Calls read from PLINK binary: $duration seconds.\n"; 
    # compare PLINK and Sequenom genotypes, and write to combined file 
    my ($countRef, $matchRef) 
        = compareGenotypes($plinkCallsRef, $sqnmCallsRef, $outDir);
    my %failedSamples 
        = writeComparisonResults($countRef, $matchRef, $missingSamplesRef, 
                                 $minCheckedSNPs, $minIdent, $outDir);
    # pairwise check on failed samples for possible swaps
    my @failedSamples = keys(%failedSamples);
    ($countRef, $matchRef) 
        = compareFailedPairs($plinkCallsRef, $sqnmCallsRef, \@failedSamples);
    writeFailedPairCheck(\@failedSamples, $countRef, $matchRef, $minIdent, 
                         $outDir);
    print $logfile "Finished.\n";
    close $logfile;
    return 1;
}



1;
