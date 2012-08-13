#!/software/bin/perl

use warnings;
use strict;
use WrapDBI;
use Getopt::Long;
use plink_binary; # in /software/varinf/gftools/lib ; front-end for C library
use WTSI::Genotyping::QC::QCPlotShared;

# Check identity of genotyped data against sequenom
# Input: files of genotypes in tab-delimited format, one row per SNP

# Author:  Iain Bancarz, ib5@sanger.ac.uk (refactored edition Feb 2012, original author unknown)

# Old version used heterozygosity mismatch rates for comparison
# Modify to use genotype mismatch rates
# Do not count "flips" and/or "swaps" as mismatches
# Flip:  Reverse complement, eg. GA and TC
# Swap:  Transpose major and minor alleles, eg. GA and AG
# can have both flip and swap, eg. GA ~ CT

my $help;
my ($outputGT, $outputResults,  $outputFail, $outputFailedPairs, $outputFailedPairsMatch, 
    $minCheckedSNPs, $minIdent, $log); # arguments to run()

GetOptions("results=s"   => \$outputResults,
           "fail=s"      => \$outputFail,
           "gt=s"        => \$outputGT,
           "min_snps=i"  => \$minCheckedSNPs,
           "min_ident=f" => \$minIdent,
           "h|help"      => \$help,
           "log=s"       => \$log);

if ($help) {
    print STDERR "Usage: $0 [ output file options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--results=PATH      Output path for full results
--fail=PATH         Output path for failures
--gt=PATH           Output path for genotypes by SNP and sample
--log=PATH          Output path for log file
--min_snps=NUMBER   Minimum number of SNPs for comparison
--min_ident=NUMBER  Minimum threshold of SNP matches for identity; 0 <= NUMBER <= 1
--help              Print this help text and exit
Unspecified options will receive default values, with output written to current directory.
";
    exit(0);
}

# parameter default values
$outputGT ||= 'identity_check_gt.txt'; 
$outputResults ||= 'identity_check_results.txt';
$outputFail ||= 'identity_check_fail.txt';
$outputFailedPairs ||= 'identity_check_failed_pairs.txt';
$outputFailedPairsMatch ||= 'identity_check_failed_pairs_match.txt';
$log ||= 'identity_check.log';
$minCheckedSNPs ||= 10;
$minIdent ||= 0.9;

run($outputGT, $outputResults,  $outputFail, $outputFailedPairs, $outputFailedPairsMatch, 
    $minCheckedSNPs, $minIdent, $log);

sub compareGenotypes {
    # read plink and (if available) sequenom genotypes by SNP and sample
    # write genotypes to file
    # also compare genotypes for equivalence and return results
    my (%sampleNames, %snpNames, @sampleNames, @snpNames, $plinkCall, $sqnmCall, %count, %match);
    my %plinkCalls = %{ shift() }; # hashes of calls by sample & snp
    my %sqnmCalls =  %{ shift() };
    my $outputGT = shift; # output path
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
    @sampleNames = sort(@sampleNames); # sorting not strictly necessary, but ensures consistent output order
    @snpNames = sort(@snpNames);
    # open output file and print headers
    open my $gt, ">", $outputGT or die $!; 
    print $gt "#".join("\t", qw(SNP sample genotype sequenom))."\n";
    # write genotypes to file and populate comparison hash
    foreach my $snp (@snpNames) {
	foreach my $sample (@sampleNames) {
	    my $plinkCall = $plinkCalls{$sample}{$snp};
	    unless ($plinkCall) { $plinkCall = '-'; }
	    my $sqnmCall = $sqnmCalls{$sample}{$snp};
	    unless ($sqnmCall) { $sqnmCall = '-'; }
	    print $gt join("\t", $snp, $sample, $plinkCall, $sqnmCall), "\n";
	    next if ($plinkCall eq '-' || $sqnmCall eq '-'); # no comparison if one GT is missing
	    $count{$sample}++;
	    my $equiv = eval { genotypesAreEquivalent($sqnmCalls{$sample}{$snp}, $plinkCall) };
	    unless (defined($equiv)) {  
		print STDERR "WARNING: ".$@; # error caught from genotypesAreEquivalent
		$equiv = 0;
	    } 
	    $match{$sample}++ if $equiv;
	}
    }
    close $gt or die $!;
    return (\%count, \%match);
}

sub compareFailedPairs {
    # do pairwise check of all failed samples (in case sample IDs were swapped in Sequenom or Illumina)
    # for samples (i, j) compare SNP calls:  (Sequenom_i, Illumina_j)
    # NOTE: in general, (Sequenom_i, Illumina_j) != (Sequenom_j, Illumina_i) on shared SNP subsets and concordance
    # So, test is not symmetric!  But for a real "failed pair", would expect high similarity on (i,j) and (j,i).
    my %plinkCalls = %{ shift() };
    my %sqnmCalls = %{ shift() };
    my @failedSamples = @{ shift() };
    my (@count, @match);
    for (my $i = 0; $i < @failedSamples; $i++) {
	for (my $j = $0; $j < @failedSamples; $j++) {
	    next if $i == $j; # (i,i) is guaranteed to match!
	    my $sample_i = $failedSamples[$i];
	    my $sample_j = $failedSamples[$j];
	    foreach my $snp (keys %{$sqnmCalls{$sample_i}}) { # start with Sequenom calls, compare to Illumina
		my $plinkCall = $plinkCalls{$sample_j}{$snp};
		next unless $plinkCall;
		$count[$i][$j] += 1;
		unless ($match[$i][$j]) { $match[$i][$j] = 0; } # ensure all counts have corresponding match entry
		my $equiv = eval { genotypesAreEquivalent($sqnmCalls{$sample_i}{$snp}, $plinkCall) };
		unless (defined($equiv)) {  
		    print STDERR "WARNING: ".$@; # error caught from genotypesAreEquivalent
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
    if ($gt0 eq $gt1 || $gt0 eq $gt1Swap || $gt0 eq revComp($gt1) || $gt0 eq revComp($gt1Swap) ) {
	return 1; # match
    } else {
	return 0; # no match
    }
}

sub getSampleNamesIDs {  
    # extract sample IDs from a plink_binary object, assuming standard PLATE_WELL_ID format
    # generate hash of IDs indexed by name; assume names are unique, IDs may not be
    # also get list of names (order of sample names in binary is significant for later tasks)
    my ($pb) = @_;  # $pb = plink_binary 
    my (%samples, @sampleNames);
    for my $i (0..$pb->{"individuals"}->size() - 1) {
	my $longName = $pb->{"individuals"}->get($i)->{"name"};
	my ($plate, $well, $id) = split /_/, $longName, 3; # assumes $longName is in standard format!
	$samples{$longName} = $id;
	push(@sampleNames, $longName);
    }
    return (\%samples, \@sampleNames);
}

sub getSequenomCallData {
    # get results from sequenom DB query, for each sample ID
    # inputs: sequenom query, hash of sample ids by sample name
    # outputs: calls by sample and SNP; indicator of SNPs found in DB; indicator of samples *not* found in DB
    my %sampleIDs = @_;
    my ($dbh, $sth, @samples, %sqnmCalls, %sqnmSnps, %missingSamples);
    ($dbh, $sth) = sequenomQueryBySample(); # database connection, and query for results by ID
    @samples = keys(%sampleIDs); # list of sample names
    my $totalCalls = 0;
    foreach my $sample (@samples) {
	$sth->execute($sampleIDs{$sample}); # query DB with sample ID
	foreach my $row (@{$sth->fetchall_arrayref}) {
	    my ($well, $snp, $call, $conf, $disregard) = @{$row};
	    next if $disregard == 1;                # skip calls with 'disregard' flag in database
	    $call .= $call if length($call) == 1;   # ensure two alleles; for seqnm genotypes "AA" may be "A"
	    next if $call =~ /[N]{2}/;              # skip 'NN' calls
	    $sqnmCalls{$sample}{$snp} = $call;      # record calls by sample and SNP
	    $sqnmSnps{$snp} = 1;                    # record SNPs found
	    $totalCalls += 1;
	}
	$missingSamples{$sample} = 1 unless $sqnmCalls{$sample}; # sample missing if no valid calls found
    }
    # clean up database objects
    $sth->finish;
    $dbh->disconnect;
    return (\%sqnmCalls, \%sqnmSnps, \%missingSamples, $totalCalls);
}

sub readPlinkCalls {
    # read genotype calls from given plink_binary object
    # requires list of sample names in same order as in plink file (TODO: make this self-contained?)
    # return hash of calls by sample and SNP name (corresponds to %sqnmCalls hash)
    my ($pb, $sampleNamesRef, $sqnmSnpsRef) = @_;
    my @sampleNames = @$sampleNamesRef;
    my %sqnmSnps = %$sqnmSnpsRef;
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    my %plinkCalls;
    my $start = time();
    while ($pb->next_snp($snp, $genotypes)) {
	my $snp_id = $snp->{"name"};
	next unless $sqnmSnps{$snp_id};
	for my $i (0..$genotypes->size() - 1) {
	    my $call = $genotypes->get($i);
	    next if $call =~ /[N]{2}/;              # skip 'NN' calls
	    $plinkCalls{$sampleNames[$i]}{$snp_id} = $call;
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

sub sequenomQueryBySample {
    # connect to Sequenom results DB, query for results by sample ID
    # return DB connection and query objects
    my $dbh = WrapDBI->connect('snp');
    my $sth = $dbh->prepare(qq(
select distinct well_assay.id_well, snp_name.snp_name,
genotype.genotype, genotype.confidence, genotype.disregard
from well_assay, snpassay_snp, snp_name, genotype, individual
where well_assay.id_assay = snpassay_snp.id_assay
and snpassay_snp.id_snp = snp_name.id_snp
and (snp_name.snp_name_type = 1 or snp_name.snp_name_type = 6)
and genotype.id_assay = snpassay_snp.id_assay
and genotype.id_ind = individual.id_ind
and disregard = 0
and confidence <> 'A'
and individual.clonename = ?
));
    return ($dbh, $sth);
}

sub writeComparisonResults {
    # write results of identity check to files (similar to old full/summary format)
    # possible results: Skipped, Pass, Fail, Unavailable
    my %count = %{ shift() }; # count of valid genotype call pairs, by sample
    my %match = %{ shift() }; # count of matching pairs, by sample
    my %missing = %{ shift() }; # samples missing from Sequenom
    my $min_checked_snps = shift;
    my $min_ident = shift;
    my $outputResults = shift;
    my $outputFailures = shift;
    my %failedSamples;
    open my $results, ">", $outputResults or die $!;
    open my $fail, ">", $outputFailures or die $!;
    my $header = join("\t", "# Sequenom identity comparison", "MIN_SNPS:$min_checked_snps", 
		      "PASS_THRESHOLD:$min_ident")."\n";
    $header .= join("\t", "# sample", "common SNPs", "matching calls", "concordance", "result")."\n";
    print $results $header;
    print $fail $header;
    # write skipped/pass/fail samples to RESULTS; fail samples to FAIL
    foreach my $sample (keys %count) {
	my $concord = $match{$sample} / $count{$sample};
	my $line = sprintf "%s\t%d\t%d\t%.4f\t",
            $sample,
            $count{$sample},
            $match{$sample},
            $concord;
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
    # write results of check on failed sample pairs (previously was appended to _full.txt file)
    # summarise for all pairwise results; details for possible swaps
    # could extend to write complete list of SNP calls for possible swaps, but not doing so for now
    my @samples = @{ shift() };
    my @count = @{ shift() };
    my @match = @{ shift() };
    my $minIdent = shift;
    my $resultsPath = shift;
    my $detailsPath = shift;
    my ($i, $j, @matchedPairs, %wroteMatch);
    my $digits = 3; # precision for output
    # do all pairwise checks and write results to file
    # populate @matchedPairs list of failed pairs with SNP call match above $minIdent threshold
    open my $results, ">", $resultsPath or die $!;
    my $header = "# Pairwise check using all samples which failed concordance, for possible ID swaps.\n";
    $header .= "# Check A:B finds Sequenom calls for B, then compares to Illumina calls for A\n";
    $header .= "# MIN_IDENTITY_FOR_MATCH:$minIdent\n";
    $header .= join("\t", "Illumina", "Sequenom", "common SNPs", "matching calls", "concordance", "result")."\n";
    print $results $header;
    for (my $i = 0; $i < @samples; $i++) {
	for (my $j = $0; $j < @samples; $j++) {
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
	    print $results join("\t",  $samples[$j], $samples[$i], $count[$i][$j], $match[$i][$j], 
			       sprintf("%.${digits}f", $match[$i][$j]/$count[$i][$j]), $status)."\n";
	}
    }
    close $results;
    # write details of matched pairs (possible swaps) to second output file
    $header = "# Details of sample pairs (S1, S2) which failed at least one pairwise check\n";
    $header .= "# Check A:B finds Sequenom calls for B, then compares to Illumina calls for A\n";
    $header .= join("\t", "# S1", "S2", "S1:S2 shared SNPs", "S1:S2 matches", "S1:S2 concordance",
		    "S2:S1 shared SNPs", "S2:S1 matches", "S2:S1 concordance")."\n"; 
    open my $detail, ">", $detailsPath or die $!;
    print $detail $header;
    foreach my $pairRef (@matchedPairs) {
	($i, $j) = @$pairRef; # $i = sequenom sample, $j = illumina sample
	if ($wroteMatch{$samples[$j]}{$samples[$i]}) { next; } # no need to write results for both (i,j) and (j,i)
	else { $wroteMatch{$samples[$i]}{$samples[$j]} = 1; }
	my @words = ($samples[$j], $samples[$i], # Illumina sample name goes first, as with RESULTS file
		     $count[$i][$j], $match[$i][$j], sprintf("%.${digits}f", $match[$i][$j]/$count[$i][$j]), 
		     $count[$j][$i], $match[$j][$i], sprintf("%.${digits}f", $match[$j][$i]/$count[$j][$i]),  
	    );
	print $detail join("\t", @words)."\n";
    }
    close $detail;
    return 1;
}

sub run {
    # 'main' method to run identity check
    # read parameters
    my ($outputGT, $outputResults, $outputFail, $outputFailedPairs, $outputFailedPairsMatch, 
	$minCheckedSNPs, $minIdent, $log) = @_;
    # initialise variables
    my ($samplesRef, $sampleNamesRef, $sqnmCallsRef, $sqnmSnpsRef, $missingSamplesRef, $sqnmTotal, $plinkCallsRef,
	$duration, $countRef, $matchRef);
    # get sample IDs by name from PLINK file 
    my $logfile;
    if ($log) { open $logfile, ">", $log || die $!; }
    my $pb = new plink_binary::plink_binary($ARGV[0]); # $pb = object to parse given PLINK files
    $pb->{"missing_genotype"} = "N"; # TODO check if this is necessary
    ($samplesRef, $sampleNamesRef) = getSampleNamesIDs($pb);
    my %samples = %$samplesRef;
    my @sampleNames = @$sampleNamesRef;
    my $size = @sampleNames;
    if ($log) { print $logfile $size." samples read from PLINK binary.\n"; }
    # get Sequenom genotypes for all samples 
    ($sqnmCallsRef, $sqnmSnpsRef, $missingSamplesRef, $sqnmTotal) = getSequenomCallData(%samples);
    if ($log) { print $logfile $sqnmTotal." calls read from Sequenom.\n"; }
    # get PLINK genotypes for all samples; can take a while!
    ($plinkCallsRef, $duration) = readPlinkCalls($pb, $sampleNamesRef, $sqnmSnpsRef);
    if ($log) { print $logfile "Calls read from PLINK binary: $duration seconds.\n"; }
    # compare PLINK and Sequenom genotypes, and write to combined file 
    ($countRef, $matchRef) = compareGenotypes($plinkCallsRef, $sqnmCallsRef, $outputGT);
    my %failedSamples = writeComparisonResults($countRef, $matchRef, $missingSamplesRef, $minCheckedSNPs, 
					       $minIdent, $outputResults, $outputFail);
    # pairwise check on failed samples for possible swaps
    my @failedSamples = keys(%failedSamples);
    ($countRef, $matchRef) = compareFailedPairs($plinkCallsRef, $sqnmCallsRef, \@failedSamples);
    writeFailedPairCheck(\@failedSamples, $countRef, $matchRef, $minIdent, 
			 $outputFailedPairs, $outputFailedPairsMatch);
    if ($log) {
	print $logfile "Finished.\n";
	close $logfile;
    }
    return 1;
}
