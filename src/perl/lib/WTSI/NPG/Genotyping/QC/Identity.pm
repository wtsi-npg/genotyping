
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


use warnings;
use strict;
use utf8;

package WTSI::NPG::Genotyping::QC::Identity;

use Moose;

use Carp;
use Cwd;
use JSON;
use List::Util qw(max);
use Log::Log4perl;
use Log::Log4perl::Level;
use POSIX qw(ceil);
use plink_binary; # in /software/varinf/gftools/lib ; front-end for C library

use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultConfigDir
                                               getDatabaseObject);
use WTSI::NPG::Genotyping::QC::SnpID qw(convertFromIlluminaExomeSNP);
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

with 'WTSI::NPG::Loggable';

has 'db_path' =>
    (is       => 'rw',
     isa      => 'Str',
     required => 1,
    );

has 'ini_path' =>
    (is        => 'rw',
     isa       => 'Str',
     required  => 1,
    );

has 'is_sequenom' =>
    (is           => 'rw',
     isa          => 'Bool',
     required     => 1,
    );

has 'min_shared_snps' =>
    (is               => 'rw',
     isa              => 'Int',
     default          => 8,
 );

has 'output_dir' =>
    (is          => 'rw',
     isa         => 'Str',
     default     => '.'
 );

has 'output_names' =>
    (is            => 'ro',
     isa           => 'HashRef[Str]',
     default       => sub {{'genotypes'  => 'identity_check_gt.txt',
                            'results'    => 'identity_check_results.txt',
                            'fail_pairs' => 'identity_check_failed_pairs.txt',
                            'json'       => 'identity_check.json' }},
 );

has 'pass_threshold' => # minimum similarity for metric pass
    (is              => 'rw',
     isa             => 'Num',
     default         => 0.9,
 );

has 'pipedb' =>
    (is      => 'rw',
     isa     => 'WTSI::NPG::Genotyping::Database::Pipeline',
 );

has 'plex_manifest' => # location of plex manifest in iRODS
    (is             => 'rw',
     isa            => 'Str',
     required       => 1,
 );

has 'plink_path' =>
    (is          => 'rw',
     isa         => 'Str',
     required    => 1,
 );

has 'plink' =>
    (is     => 'rw',
     isa    => 'plink_binary::plink_binary',
 );

has 'swap_threshold' => # minimum similarity to be flagged as possible swap
    (is              => 'rw',
     isa             => 'Num',
     default         => 0.9,
);


sub BUILD {
  my ($self,) = @_;
  $self->plink(plink_binary::plink_binary->new($self->plink_path));
  $self->plink->{"missing_genotype"} = "N";
  $self->pipedb(getDatabaseObject($self->db_path, $self->ini_path));
  $self->logger->level($WARN);
}

sub compareFailedPairs {
    # pairwise check of all failed samples; use output to detect swaps
    # Consider sample pair (i, j)
    # let s_ij = rate of matching calls between (Illumina_i, Sequenom_j)
    # we may have s_ij != s_ji, so define pairwise metric as max(s_ij, s_ji)
    my ($self, $gtRef, $failRef, $snpRef) = @_;
    my %genotypes = %{$gtRef};
    my @failedSamples = @{$failRef};
    my @snps = @{$snpRef};
    my @comparison = ();
    for (my $i = 0; $i < @failedSamples; $i++) {
        for (my $j = 0; $j < $i; $j++) {
            my $sample_i = $failedSamples[$i];
            my $sample_j = $failedSamples[$j];
	    my @match = (0,0);
	    foreach my $snp (@snps) {
		my ($plink_i, $plex_i) = @{$genotypes{$sample_i}{$snp}};
		my ($plink_j, $plex_j) = @{$genotypes{$sample_j}{$snp}};
		my $equiv_ij = eval { $self->equivalent($plink_i, $plex_j) };
		my $equiv_ji = eval { $self->equivalent($plink_j, $plex_i) };
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
    my ($self, $gt0, $gt1) = @_;
    # basic sanity checking on input
    # allow no-call genotypes (represented by NN or 0)
    my $inputOK = 1;
    foreach my $gt ($gt0, $gt1) {
	if ($gt && $gt ne 'NN' && (length($gt)!=2 || $gt =~ /[^ACGT]/)) { 
	    $inputOK = 0; 
	}
    }
    unless ($inputOK) { 
        $self->logger->logcroak("Incorrect arguments to equivalentGenotype:",
                                $gt0, $gt1);
    }
    my $gt1Swap = join('', reverse(split('', $gt1))); # swap alleles
    if ($gt0 eq $gt1 || $gt0 eq $gt1Swap || $gt0 eq $self->revComp($gt1) ||
        $gt0 eq $self->revComp($gt1Swap) ) {
        return 1; # match
    } else {
        return 0; # no match
    }
}

sub findIdentity {
    # find the identity metric for each sample
    # return: metric values, genotypes by SNP & sample, pass/fail status
    my ($self, $plinkRef, $plexRef, $snpsRef) = @_;
    my %plink = %{$plinkRef};
    my %qcplex = %{$plexRef};
    my @snps = @{$snpsRef};
    my (%identity, %genotypes, %failed, %missing);
    foreach my $sample (keys(%plink)) {
	my $match = 0;
	# compare genotypes
	# mark samples as missing OR pass/fail (but not both)
	if ($qcplex{$sample}) {
	    foreach my $snp (@snps) {
		my $pCall = $plink{$sample}{$snp};
		my $sCall = $qcplex{$sample}{$snp};
		if ($pCall && $sCall) {
		    my $equiv = eval { $self->equivalent($pCall, $sCall) };
		    unless (defined($equiv)) {
			$self->logger->logwarn("WARNING: ".$@); # error caught
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
	    if ($id < $self->pass_threshold) { $failed{$sample} = 1; }
	} else {
	    $missing{$sample} = 1;
	    $identity{$sample} = 0;
	}
    }
    return (\%identity, \%genotypes, \%failed, \%missing);
}

sub getIntersectingSNPsPlink {
    # find SNPs in Plink data which are also in QC plex
    # TODO modify to get plex file from IRODS
    # find Plink SNP names and cross-reference with Sequenom/Fluidigm
    my ($self,) = @_;
    my @plinkSNPs;
    for my $i (0..$self->plink->{"snps"}->size() - 1) {
	my $name = $self->plink->{"snps"}->get($i)->{"name"};
	push @plinkSNPs, $name;
    }
    return $self->getPlexIntersection(\@plinkSNPs);
}

sub getPlexIntersection {
    # find intersection of given SNP list with QC plex
    my ($self, $compareRef) = @_;
    my @compare = @{$compareRef};
    my $irods = WTSI::NPG::iRODS->new;
    my $data_object = WTSI::NPG::iRODS::DataObject->new
        ($irods, $self->plex_manifest);
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);
    $snpset->logger->level($WARN);
    my %plexSNPs = ();
    foreach my $name ($snpset->snp_names) { $plexSNPs{$name} = 1; } 
    my @shared;
    foreach my $name (@compare) {
        $name = convertFromIlluminaExomeSNP($name);
	if ($plexSNPs{$name}) { push(@shared, $name); }
    }
    return @shared;
}

sub getSampleNamesIDs {
    # extract sample IDs from a plink_binary object
    # first, try parsing sampleName in standard PLATE_WELL_ID format
    # if unsuccessful, set sample ID = sampleName
    # output hash of IDs indexed by name
    # also get list of names (use to ensure consistent name order)
    my ($self,) = @_;
    my (%samples, @sampleNames);
    for my $i (0..$self->plink->{"individuals"}->size() - 1) {
        my $longName = $self->plink->{"individuals"}->get($i)->{"name"};
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
    my ($self,) = @_;
    my $irods = WTSI::NPG::iRODS->new;
    my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, $self->plex_manifest);
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);
    $snpset->logger->level($WARN);
    return $snpset->snp_names;
}

sub readPlexCalls {
    # read QC plex calls (Sequenom or Fluidigm) from pipeline SQLite database
    # return a hash of calls indexed by sample and SNP
    my ($self, ) = @_;
    # read samples and SNP names
    my @samples = $self->pipedb->sample->all;
    $self->logger->debug("Read ", scalar(@samples),
                         " samples from pipeline DB");
    my @snps = $self->pipedb->snp->all;
    my $snpTotal = @snps;
    $self->logger->debug("Read $snpTotal SNPs from pipeline DB");
    my %snpNames;
    foreach my $snp (@snps) {
	$snpNames{$snp->id_snp} = $snp->name;
    }
    # read QC calls for each sample and SNP
    my $snpResultTotal = 0;
    my %results;
    my $i = 0;
    foreach my $sample (@samples) {
        if ($sample->include == 0) { next; }
        my $sampleURI = $sample->uri;
        my @results = $sample->results->all;
        $i++;
        if ($i % 100 == 0) {
            $self->logger->debug("Read ", scalar(@results),
                                 " results for sample ", $i, " of ",
                                 scalar(@samples));
        }
        foreach my $result (@results) {
            my @snpResults = $result->snp_results->all;
            $snpResultTotal += @snpResults;
            foreach my $snpResult (@snpResults) {
                my $snpName = $snpNames{$snpResult->id_snp};
                if (!$results{$sampleURI}{$snpName}) {
                    $results{$sampleURI}{$snpName} = $snpResult->value;
                }
            }
        }
    }
    $self->logger->debug("Read ", $snpResultTotal,
                         " QC SNP results from pipeline DB");
    return \%results;
}

sub readPlinkCalls {
    # read genotype calls by sample & snp from given plink_binary object
    # requires list of sample names in same order as in plink file
    # assumes that "sample names" in the Plink dataset are URI's
    # return hash of calls by sample and SNP name
    my ($self, $sampleNamesRef, $snpsRef) = @_;
    my @sampleNames = @$sampleNamesRef;
    my @snps = @$snpsRef;
    my %snps;
    foreach my $snp_id (@snps) { $snps{$snp_id} = 1; }
    my $snp = new plink_binary::snp;
    my $genotypes = new plink_binary::vectorstr;
    my %plinkCalls;
    while ($self->plink->next_snp($snp, $genotypes)) {
        # read calls from Plink binary object
        # try both "plink" and "sequenom" SNP name formats
        my $snp_id_illumina = $snp->{"name"};
        my $snp_id_sequenom = convertFromIlluminaExomeSNP($snp_id_illumina);
        foreach my $snp_id ($snp_id_illumina, $snp_id_sequenom) {
            if (!$snps{$snp_id}) { next; }
            for my $i (0..$genotypes->size() - 1) {
                my $call = $genotypes->get($i);
		if ($call =~ /[N]{2}/) { $call = 0; } 
                $plinkCalls{$sampleNames[$i]}{$snp_id} = $call;
            }
        }
    }
    return \%plinkCalls;
}

sub revComp {
    # reverse complement a DNA sequence
    my ($self, $seq) = @_;
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

sub writeJson {
    # get data structure for output to and write to JSON file
    # first argument is hash of values (if check was run) or list of samples (if check was not run)
    my ($self, $resultsRef, $idCheck, $minSnps, $commonSnps) = @_;
    my $idRef;
    my %data = (results => $resultsRef,
		identity_check_run => $idCheck,
		min_snps => $minSnps,
		common_snps => $commonSnps
	);
    my $outPath = $self->output_dir.'/'.$self->output_names->{'json'};
    open my $out, ">", $outPath || 
        $self->logger->logcroak("Cannot open '$outPath'");
    print $out encode_json(\%data);
    close $out || $self->logger->logcroak("Cannot close '$outPath'");
}

sub run_identity_check {
    # 'main' method to run identity check
    my ($self,) = @_;
    # 1) Read sample names and IDs from Plink
    my ($samplesRef, $sampleNamesRef) = $self->getSampleNamesIDs();
    $self->logger->debug("Sample names read from PLINK binary.\n");
    # definitive list of qc SNPs
    my @snps = $self->getIntersectingSNPsPlink();
    my $snpTotal = @snps;
    if ($snpTotal < $self->min_shared_snps) {
	my %id;
	foreach my $sample (@{$sampleNamesRef}) { $id{$sample} = 'NA'; }
	$self->writeJson(\%id, 0, $self->min_shared_snps, $snpTotal);
	$self->logger->logwarn("Cannot do identity check; ",
                               $self->min_shared_snps,
                               " SNPs from QC plex required ", $snpTotal,
                               " found");
    } else {
	# 2) Read Sequenom results from pipeline SQLite DB
	my $start = time();
	my $plexCallsRef = $self->readPlexCalls();
	my $duration = time() - $start;
	$self->logger->debug("Calls read from pipeline DB: ",
                             $duration, " seconds.\n");
	# 3) Read PLINK genotypes for all samples; can take a while!
	$start = time();
	my $plinkCallsRef = $self->readPlinkCalls($sampleNamesRef, \@snps);
	$duration = time() - $start;
	$self->logger->debug("Calls read from PLINK binary: ",
                             $duration, " seconds.\n");
	# 4) Find identity, genotypes, and pass/fail status; write output
	my ($idRef, $gtRef, $failRef, $missingRef) = $self->findIdentity($plinkCallsRef, $plexCallsRef, \@snps, $self->pass_threshold);
	$self->writeJson($idRef, 1, $self->min_shared_snps, $snpTotal);
	$self->writeGenotypes($gtRef, \@snps);
	$self->writeIdentity($idRef, $failRef, $missingRef, $sampleNamesRef,
                             $snpTotal, $self->pass_threshold);
	# 5) Pairwise check on failed samples for possible swaps
	my @failed = sort(keys(%{$failRef}));
	my $compareRef = $self->compareFailedPairs($gtRef, \@failed,
                                                   \@snps,
                                                   $self->swap_threshold);
	$self->writeFailedPairComparison($compareRef, $self->pass_threshold);
	$self->logger->debug("Finished identity check.\n");
    }
    return 1;
}

no Moose;

1;



__END__

=head1 NAME

WTSI::NPG::Genotyping::QC::Identity

=head1 DESCRIPTION

Class to run the WTSI Genotyping pipeline identity check on a Plink dataset.
Checks Plink calls against a QC plex (Sequenom or Fluidigm) by comparing
calls on SNPs which occur in both the Infinium and QC plex manifests. If
available SNPs are too few, omit the identity check. No-calls on any given
sample are counted as mismatches. Samples with concordance below a given
threshold fail the QC metric. Failed pairs of samples are compared in order
to detect possible swaps.

Output is a JSON file with identity metric for each sample, and supplementary
text files.

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
