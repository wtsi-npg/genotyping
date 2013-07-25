#!/software/bin/perl

#
# Copyright (c) 2013 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# Parse a SNP manifest .csv file and write metadata in .json format
# Input: The manifest
# Outputs:
# 1. SNP manifest, converted to .json format
# 2. Chromosome boundaries with respect to position in SNP manifest

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use JSON;

Log::Log4perl->easy_init($ERROR);

sub byChromosomePosition {
    # use to sort manifest into (chromosome, position) order
    # sort chromosomes by string order, positions by numeric order
    # $a, $b are global variables used by Perl sort
    my %snpA = %$a;
    my %snpB = %$b;
    my $result = $snpA{'chromosome'} cmp $snpB{'chromosome'} 
                 || $snpA{'position'} <=> $snpB{'position'};
    return $result;
}

sub findChromosomeBounds {
    # find boundaries (start/finish indices) of chromosomes wrt sorted manifest
    my @manifest = @_;
    my %chromosomes = ();
    my @bounds = ();
    my $start = 0;
    my $lastChr = 'NULL';
    for (my $i=0; $i<@manifest; $i++) {
        my $chr = $manifest[$i]{'chromosome'};
        if ($lastChr ne 'NULL' && $chr ne $lastChr) { # end previous chromosome
            push (@bounds, { 'chromosome' => $lastChr,
                             'start' => $start,
                             'end' => $i });
            $start = $i;
            if ($chromosomes{$chr}) {
                croak("Inconsistent chromosome name at position $i: $chr\n");
            } else {
                $chromosomes{$chr} = 1;
            }
        }
        if ($i+1==@manifest) { # end of final chromosome
            push (@bounds, { 'chromosome' => $chr,
                             'start' => $start,
                             'end' => $i+1 });
        }
        $lastChr = $chr;
    }
    return @bounds;
}

sub readManifest {
    # read manifest bpm.csv
    # .csv fields: Index,Name,Chromosome,Position,GenTrain Score,SNP,ILMN Strand,Customer Strand,NormID
    # for each SNP, parse: Name,Chromosome,Position,AlleleA,AlleleB,NormID
    my $inPath = shift;
    my $verbose = shift;
    my @manifest;
    my %indices = (name => 1,
                   chromosome => 2,
                   position => 3,
                   snp => 5,
                   norm_id => 8
        );
    my $i = 0;
    open my $in, "<", $inPath || croak "Cannot read input path $inPath";
    while (<$in>) {
        $i++;
        if ($verbose && $i % 10000 == 0) { print "$i lines read.\n"; }
        if ($i == 1) { next; } # first line is header
        $_ =~ s/\s+$//g; # remove whitespace (including \r) from end of line
        my %snp;
        my @fields = split /,/;
        foreach my $key (qw/name chromosome position norm_id/) {
            $snp{$key} = $fields[$indices{$key}];
        }
        my $alleles = $fields[$indices{'snp'}];
        $alleles =~ s/\W//g; # remove nonword characters
        my @alleles = split('', $alleles);
        if (@alleles!=2) { 
            croak "Failed to find 2 alleles at manifest line ".$i."\n"; 
        }
        foreach my $allele (@alleles) {
            if ($allele !~ /[A-Z]/) { # may include letters other than ACGT
                croak "Invalid allele character at manifest line ".$i."\n";
            }
        }
        $snp{'allele_a'} = $alleles[0];
        $snp{'allele_b'} = $alleles[1];
        # change chromosome ID to satisfy plink convention
        my $key = 'chromosome';
        if ($snp{$key} eq 'X') { $snp{$key}=23; }
        elsif ($snp{$key} eq 'Y') { $snp{$key}=24; }
        elsif ($snp{$key} eq 'XY') { $snp{$key}=25; }
        elsif ($snp{$key} eq 'MT') { $snp{$key}=26; }
        push(@manifest, \%snp);
    }
    close $in || croak "Cannot close input path $inPath";
    return @manifest;
}

sub sortManifest {
    # must be done before finding chromosome boundaries
    return sort byChromosomePosition @_;
}

sub run {
    my ($manifest, $chrJson, $snpJson, $verbose, $out, $start);
    $start = time();
    GetOptions('manifest=s' => \$manifest,
               'chromosomes=s' => \$chrJson,
               'snp=s'=> \$snpJson,
               'verbose' => \$verbose,
        );
    unless (-e $manifest) {
        croak("Manifest file \"$manifest\" does not exist");
    }
    my @manifest = sortManifest(readManifest($manifest, $verbose));
    if ($snpJson) {
        open $out, ">", $snpJson || croak "Cannot open output $snpJson";
        print $out to_json(\@manifest);
        close $out || croak "Cannot close output $snpJson";
    }
    if ($chrJson) {
        my @bounds = findChromosomeBounds(@manifest);
        open $out, ">", $chrJson || croak "Cannot open output $chrJson";
        print $out to_json(\@bounds);
        close $out || croak "Cannot close output $chrJson";
    }
    my $duration = time() - $start;
    if ($verbose) { print "Finished. Duration: $duration s.\n"; }
}

run();
