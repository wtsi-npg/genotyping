# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# methods to translate between Illumina and Sequenom/Fluidigm SNP
# naming conventions

package WTSI::NPG::Genotyping::QC::SnpID;

use strict;
use warnings;
use Carp;
use Exporter;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/convertFromIlluminaExomeSNP convertToIlluminaExomeSNP/;

our $VERSION = '';

sub convertFromIlluminaExomeSNP {
    # strip off exm- prefix, if any
    my $id = shift;
    my $pattern = '^exm-';
    my $newID;
    if ($id =~ m{$pattern}msx) {
        my @items = split /$pattern/msx, $id;
        $newID = pop(@items);
      } else {
        $newID = $id;
    }
    return $newID;
}

sub convertToIlluminaExomeSNP {
    # prepend exm- prefix, if not already present
    my $id = shift;
    my $prefix = 'exm-';
    my $newID;
    if ($id =~ m{^$prefix}msx) {
        $newID = $id;
    } else {
        $newID = $prefix.$id;
    }
    return $newID;
}

1;
