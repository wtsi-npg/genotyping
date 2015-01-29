#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Cwd;
use Getopt::Long;
use JSON;

# convert JSON output from check_identity_bed.pl to old-style text files

my ($input, $summary, $failures, $genotypes, $help);

GetOptions("in=s"         => \$input,
	   "summary=s"    => \$dbPath,
           "failures=s"   => \$configPath,
           "genotypes=s"  => \$iniPath,
           "h|help"       => \$help);


__END__

=head1 NAME

WTSI::NPG::Genotyping::QC::Identity

=head1 DESCRIPTION

Script to convert JSON output from check_identity_bed.pl to old-style
text files. Outputs are tab-delimited text and include a summary, list of
failed samples, and list of all genotype calls used for identity.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
