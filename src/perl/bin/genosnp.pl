#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd;
use File::Basename;
use File::Temp qw(tempdir);
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;

use WTSI::Genotyping qw(read_snp_json write_gs_snps);

Log::Log4perl->easy_init($ERROR);

my $input;
my $cutoff;
my $executable;
my $output;
my $plink;
my $snps;
my $verbose;

GetOptions('cutoff=s' => \$cutoff,
           'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
           'input=s' => \$input,
           'output=s' => \$output,
           'plink' => \$plink,
           'snps=s' => \$snps,
           'verbose' => \$verbose);

unless ($snps) {
  pod2usage(-msg => "A --snps argument is required\n",
            -exitval => 2);
}

if (defined $plink && !defined $output) {
  pod2usage(-msg => "An --output argument must be given if --plink is specified\n",
            -exitval => 2);
}

unless (defined $cutoff) {
  $cutoff = 0.7;
}

$executable = 'GenoSNP';
$input ||= '/dev/stdin';
$output ||= '/dev/stdout';

my $tmp_dir = tempdir(CLEANUP => 1);
my $snps_file = $tmp_dir . '/' . 'genosnp_snps';
open (my $genosnp_snps, '>', "$snps_file")
  or die "Failed to open '$snps_file': $!\n";
write_gs_snps($genosnp_snps, [read_snp_json($snps)]);
close($genosnp_snps) or warn "Failed to close '$snps_file'\n";

my @command = ($executable, '-cutoff', $cutoff,
               '-samples', $input, '-snps', $snps_file);

if ($plink) {
  push(@command, '-bed', $output);
  # Maybe muffle GenoSNP's STDOUT chatter
  unless ($verbose) {
    push(@command, "> /dev/null");
  }

  my $command = join(' ', @command);
  if ($verbose) {
    print STDERR "Executing '$command'\n";
  }

  system($command) == 0 or die "Failed to execute '$command'\n";

  exit(0);
}
else {
  push(@command, '-calls', $output);

  my $command = join(' ', @command);
  if ($verbose) {
    print STDERR "Executing '$command'\n";
  }

  system($command) == 0 or die "Failed to execute '$command'\n";
}


__END__

=head1 NAME

genosnp - run the GenoSNP genotype caller

=head1 SYNOPSIS

genosnp [--plink --snps <filename>] < intensities > genotypes

Options:

  --snps     A JSON file of SNP annotation used to populate Plink output
             annotation. Required in combination with the --plink option.
  --help     Display help.
  --input    The GenoSNP intensity file to be read. Optional, defaults
             to STDIN.
  --output   The GenoSNP genotype file to be written. Optional,
             defaults to STDOUT.
  --plink    Write Plink BED format output. Optional, requires --output to
             be a file.
  --verbose  Print messages while processing. Optional.

=head1 DESCRIPTION

The script wraps the GenoSNP genotype caller to allow it to operate
via STDIN and STDOUT with a minimum of fuss.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
