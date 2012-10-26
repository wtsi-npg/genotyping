# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# methods to translate between Illumina and Sequenom SNP naming conventions

package WTSI::Genotyping::QC::SnpID;

use strict;
use warnings;
use Carp;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/illuminaToSequenomSNP sequenomToIlluminaSNP/;

sub illuminaToSequenomSNP {
    # strip off exm- prefix, if any
    my $id = shift;
    my $pattern = '^exm-';
    my $newID;
    if ($id =~ /$pattern/) {
        my @items = split(/$pattern/, $id);
        $newID = pop(@items);
    } else {
        $newID = $id;
    }
    return $newID;
}

sub sequenomToIlluminaSNP {
    # prepend exm- prefix, if not already present
    my $id = shift;
    my $prefix = 'exm-';
    my $newID;
    if ($id =~ /^$prefix/) {
        $newID = $id;
    } else {
        $newID = $prefix.$id;
    }
    return $newID;
}
