
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


use utf8;

package WTSI::NPG::Genotyping::QC_wip::Identity;

use Moose;

use JSON;
use List::Util qw(max);
use Log::Log4perl;
use Log::Log4perl::Level;
use plink_binary; # in /software/varinf/gftools/lib ; front-end for C library

use WTSI::NPG::Genotyping::Database::SNP;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultConfigDir
                                               getDatabaseObject);
use WTSI::NPG::Genotyping::QC::SnpID qw(convertFromIlluminaExomeSNP);
use WTSI::NPG::Genotyping::SNPSet;

with 'WTSI::DNAP::Utilities::Loggable';

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

has 'plex_manifest' => # location of plex manifest in local filesystem
    (is             => 'rw',
     isa            => 'Str',
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

our $SEQUENOM = 'Sequenom';
our $FLUIDIGM = 'Fluidigm';
our $PLEX_DIR = '/nfs/srpipe_references/genotypes';
our %PLEX_MANIFESTS = (
    $FLUIDIGM => {
        qc => $PLEX_DIR.'/qc_fluidigm_snp_info_1000Genomes.tsv',
    },
    $SEQUENOM => {
        W30467 => $PLEX_DIR.'/W30467_snp_set_info_1000Genomes.tsv',
        W34340 => $PLEX_DIR.'/W34340_snp_set_info_1000Genomes.tsv',
        W35540 => $PLEX_DIR.'/W35540_snp_set_info_1000Genomes.tsv',
    },
);

sub BUILD {
  my ($self,) = @_;
  $self->plink(plink_binary::plink_binary->new($self->plink_path));
  $self->plink->{"missing_genotype"} = "N";
  $self->pipedb(getDatabaseObject($self->db_path, $self->ini_path));
  my $sequenomTotal = $self->pipedb->total_results_for_method($SEQUENOM);
  my $fluidigmTotal = $self->pipedb->total_results_for_method($FLUIDIGM);
  my $method;
  # if no valid QC plex found, plex_manifest attribute remains undefined
  if ($sequenomTotal==0 && $fluidigmTotal==0) {
      $self->logger->warn('No QC plex results in pipeline DB');
  } elsif ($sequenomTotal!=0 && $fluidigmTotal!=0) {
      $self->logger->warn('Results for more than one QC plex in pipeline DB');
  } elsif ($fluidigmTotal != 0) {
      $method = $FLUIDIGM;
  } else {
      $method = $SEQUENOM;
  }
  if ($method) {
      my @names = @{$self->pipedb->snpset_names_for_method($method)};
      if (scalar(@names)!=1) {
          $self->logcroak("Must have exactly one snpset name ",
                          "for identity check");
      } else {
          $self->plex_manifest($PLEX_MANIFESTS{$method}{$names[0]});
      }
  }
}

sub compareFailedPairs {
    # input: data structure output by findIdentity
    # pairwise check of all failed samples; use output to detect swaps
    # Consider sample pair (i, j)
    # let s_ij = rate of matching calls between (Illumina_i, Sequenom_j)
    # we may have s_ij != s_ji, so define pairwise metric as max(s_ij, s_ji)
    my ($self, $resultRef, $snpTotal) = @_;
    my %results = %{$resultRef};
    my @failedSamples;
    foreach my $sample (keys(%results)) {
        if ($results{$sample}{'failed'}) { push @failedSamples, $sample; }
    }
    my @comparison = ();
    my $total_warnings = 0;
    for (my $i = 0; $i < @failedSamples; $i++) {
        for (my $j = 0; $j < $i; $j++) {
            my $sample_i = $failedSamples[$i];
            my $sample_j = $failedSamples[$j];
            my %gt_i = %{$results{$sample_i}{'genotypes'}};
            my %gt_j = %{$results{$sample_j}{'genotypes'}};
	    my @match = (0,0);
            foreach my $snp (keys(%gt_i)) {
                my ($plink_i, $plex_i) = @{$gt_i{$snp}};
		my ($plink_j, $plex_j) = @{$gt_j{$snp}};
		my $equiv_ij = $self->equivalent($plink_i, $plex_j);
		my $equiv_ji = $self->equivalent($plink_j, $plex_i);
		if ($equiv_ij) { $match[0]++; }
		if ($equiv_ji) { $match[1]++; }
            }
            my $similarity = max(@match)/$snpTotal;
            my $warning = 0;
            if ($similarity > $self->swap_threshold) {
                $warning = 1;
                $total_warnings++;
            }
            push(@comparison, [$sample_i, $sample_j, $similarity, $warning]);
        }
    }
    if ($total_warnings > 0) {
        $self->warn("Warning of possible sample swap for ",
                    "$total_warnings pairs of failed samples.");
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
        $self->warn("Invalid genotype arguments: '$gt0', '$gt1'");
        return 0; # no match
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
    # return hashref with identity results. For each sample:
    # - metric value
    # - hash of genotypes by SNP
    # - missing status
    # - pass/fail status
    my ($self, $plinkRef, $plexRef, $snpsRef) = @_;
    my %plink = %{$plinkRef};
    my %qcplex = %{$plexRef};
    my @snps = @{$snpsRef};
    my %identity_results;
    my $totalMissing = 0;
    foreach my $sample (keys(%plink)) {
	# compare genotypes; if missing, pass/fail status is undefined
        my %genotypes = ();
        my ($identity, $missing, $failed);
	my $match = 0;
	if ($qcplex{$sample}) {
            $missing = 0;
	    foreach my $snp (@snps) {
		my $plCall = $plink{$sample}{$snp};
		my $qcCall = $qcplex{$sample}{$snp};
		if ($plCall && $qcCall) {
		    my $equiv = $self->equivalent($plCall, $qcCall);
		    unless (defined($equiv)) {
			$self->logger->logwarn("WARNING: ".$@); # error caught
			$equiv = 0;
		    }
		    if ($equiv) { $match++; }
		}
		$plCall ||= 0;
		$qcCall ||= 0;
		$genotypes{$snp} = [$plCall, $qcCall];
	    }
	    $identity = $match / @snps;
	    if ($identity < $self->pass_threshold) { $failed = 1; }
            else { $failed = 0; }
	} else {
            # $identity, $failed undefined; %genotypes empty
	    $missing = 1;
            $totalMissing++;
	}
        $identity_results{$sample} = {'identity'  => $identity,
                                      'failed'    => $failed,
                                      'missing'   => $missing,
                                      'genotypes' => \%genotypes };
    }
    my $totalSamples = scalar(keys(%plink));
    if ($totalMissing == 0) {
        $self->info("Found identity for $totalSamples samples.");
    } else {
        $self->warn("Finished finding identity; QC plex information ",
                    "missing for $totalMissing of $totalSamples samples.");
    }
    return \%identity_results;
}

sub getIntersectingSNPsPlink {
    # find SNPs in Plink data which are also in QC plex (if any)
    my ($self,) = @_;
    my @shared;
    if ($self->plex_manifest) {
        my @plinkSNPs;
        for my $i (0..$self->plink->{"snps"}->size() - 1) {
            my $name = $self->plink->{"snps"}->get($i)->{"name"};
            push @plinkSNPs, $name;
        }
        my $snpset = WTSI::NPG::Genotyping::SNPSet->new($self->plex_manifest);
        my %plexSNPs = ();
        foreach my $name ($snpset->snp_names) { $plexSNPs{$name} = 1; }
        foreach my $name (@plinkSNPs) {
            $name = convertFromIlluminaExomeSNP($name);
            if ($plexSNPs{$name}) { push(@shared, $name); }
        }
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

sub readPlexCalls {
    # read QC plex calls (Sequenom or Fluidigm) from pipeline SQLite database
    # return a hash of calls indexed by sample and SNP
    my ($self, ) = @_;
    # read samples and SNP names
    my @samples = $self->pipedb->sample->search({include => 1});
    $self->logger->debug("Read ", scalar(@samples),
                         " samples marked for inclusion from pipeline DB");
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
        my $sampleURI = $sample->uri;
        # FIXME - Add an attribute to allow method.name(s) to be specified
        my @results = $sample->results->search
            ({'method.name' => ['Fluidigm', 'Sequenom']},
             {join => 'method'});
        $i++;
        if ($i % 100 == 0) {
            $self->logger->debug("Read ", scalar(@results),
                                 " results for sample ", $i, " of ",
                                 scalar(@samples));
        }
        foreach my $result (@results) {
            my @snpResults = $result->snp_results;
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

sub writeJson {
    # get data structure for output to and write to JSON file
    # first argument is hash of values (if check was run) or list of samples (if check was not run)
    my ($self, $resultsRef, $idCheck, $commonSnps, $swapWarnings) = @_;
    my $idRef;
    my %data = (results => $resultsRef,
                swap_warnings => $swapWarnings,
		identity_check_run => $idCheck,
		common_snps => $commonSnps,
		min_snps => $self->min_shared_snps
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
	my %idResult = ();
        my %failComparison = ();
	$self->writeJson(\%idResult, 0, $snpTotal, \%failComparison);
	$self->warn("Cannot do identity check; ",
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
	my $idResultRef = $self->findIdentity($plinkCallsRef,
                                              $plexCallsRef,
                                              \@snps);
        my $failComparisonRef = $self->compareFailedPairs($idResultRef,
                                                          scalar(@snps));
	$self->writeJson($idResultRef, 1, $snpTotal, $failComparisonRef);
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

Copyright (c) 2014-15 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
