
# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2015

# Copyright (c) 2015 Genome Research Ltd. All rights reserved.
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

# Methods for post-processing JSON results from the identity check:
# - Merge results of two or more identity checks
# - Convert from JSON to CSV, for readability & import to R/spreadsheets
# - Keep distinct results of multiple checks on the same SNP/sample pair

package WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess;

use utf8;
use strict;
use warnings;

use Moose;

#use Data::Dumper; # for testing only

use File::Slurp qw/read_file/;
use List::MoreUtils qw/uniq/;
use JSON;
use Text::CSV;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';


#=head2 _mergeGenotypes
#
#  Arg [1]    : HashRef[ArrayRef], where ArrayRef entries are JSON-spec data
#               structures output by the identity check. Keys of the HashRef
#               are names for each data structure.
#  Example    : my $merged = $id_post_process->mergeGenotypes($all_results);
#  Description: Merge of two or more identity result JSON structures. Record
#               calls (if any) for each sample/SNP pair, in each input
#               dataset. If a given sample/SNP pair is absent from one of the
#               datasets (eg. because of differing SNP panels for QC), record
#               as 'NA' (which is distinct from 'NN' for no call).
#  Returntype : ArrayRef[ArrayRef[Str]], suitable for output as CSV. First
#               row contains CSV column headers.
#
#=cut

sub _mergeGenotypes {
    my ($self, $resultsRef) = @_;
    my %results = %{$resultsRef};
    my @resultNames = sort(keys(%results));
    my @sampleNames; # want to preserve order of sample names
    my %snpNames; # using a hash instead of array to avoid memory issues
    my %mergedCalls;
    foreach my $resultName (@resultNames) {
        my @result = @{$results{$resultName}};
        foreach my $sample (@result) {
            my $sampleName = $sample->{'sample_name'};
            push(@sampleNames, $sampleName);
            my %genotypes = %{$sample->{'genotypes'}};
            foreach my $snp (keys(%genotypes)) {
                $snpNames{$snp} = 1;
                $mergedCalls{$sampleName}{$snp}{$resultName} =
                    $genotypes{$snp};
            }
        }
    }
    @sampleNames = uniq @sampleNames;
    my @snps = sort(keys(%snpNames));
    # create structure for CSV output
    # raise an error on conflicting production calls
    my @mergeHeaders = qw/sample_name snp_name production_call/;
    push(@mergeHeaders, @resultNames);
    my @merged = (\@mergeHeaders, );
    foreach my $sample (@sampleNames) {
        foreach my $snp (@snps) {
            # each sample/SNP pair is one 'row' of output
            my @mergedSnpCalls = ($sample, $snp);
            my $productionCall;
            my @qcCalls = ();
            foreach my $resultName (@resultNames) {
                my $callsRef = $mergedCalls{$sample}{$snp}{$resultName};
                if (defined($callsRef)) {
                    my ($p, $q) = @{$callsRef}; # production & QC calls
                    if (defined($productionCall) && $p ne $productionCall) {
                        $self->logcroak("Conflicting production calls for ",
                                        "sample ", $sample, ", SNP ", $snp);
                    } else {
                        $productionCall = $p;
                    }
                    push(@qcCalls, $q);
                } else {
                    push(@qcCalls, 'NA');
                }
            }
            $productionCall ||= 'NA';
            push(@mergedSnpCalls, $productionCall);
            push(@mergedSnpCalls, @qcCalls);
            push(@merged, \@mergedSnpCalls);
        }
    }
    return \@merged;
}

=head2 mergeIdentityFiles

  Arg [1]    : HashRef[Str], giving (named) JSON input paths.
  Arg [2]    : Str, path for CSV output
  Example    : $id_post_process->mergeIdentityFiles($all_result_paths,
                                                    $outpath);
  Description: 'Main' method to read named JSON paths which contain the
               results of one or more identity checks, and
               output a CSV file with merged genotype calls.

               Each JSON input is an array of hashes, one for each sample.
               The hash structure is defined in the to_json_spec method of
               SampleIdentity.pm.
  Returntype : Int

=cut

sub mergeIdentityFiles {
    my ($self, $inPathsRef, $outPath) = @_;
    my %inPaths = %{$inPathsRef}; # hash of (named) JSON input paths
    my %allResults;
    foreach my $resultName (keys(%inPaths)) {
        my $result = from_json(read_file($inPaths{$resultName}));
        $allResults{$resultName} = $result;
    }
    my $merged = $self->_mergeGenotypes(\%allResults);
    $self->_writeMergedCsv($merged, $outPath);
}


#=head2 _writeMergedCsv
#
#  Arg [1]    : ArrayRef[ArrayRef[Str]], as output by _mergeGenotypes
#  Arg [2]    : Str, path for CSV output
#  Example    : $id_post_process->writeMergedCsv($merged_results, $outpath);
#  Description: Write merged genotype calls in CSV format to the given path
#  Returntype : Int
#
#=cut

sub _writeMergedCsv {
    # take output from merge() and write in CSV format
    my ($self, $merged, $outPath) = @_;
    my $csv = Text::CSV->new({eol              => "\n",
                              allow_whitespace => undef,
                              quote_char       => undef});
    open my $out, ">", $outPath || \
        $self->logcroak("Cannot open output '$outPath'");
    foreach my $rowRef (@{$merged}) {
        $csv->print($out, $rowRef);
    }
    close $out || $self->logcroak("Cannot close output '$outPath'");
}

no Moose;

1;
