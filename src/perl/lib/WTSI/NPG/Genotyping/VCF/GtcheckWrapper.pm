
use utf8;

package WTSI::NPG::Genotyping::VCF::GtcheckWrapper;

use JSON;
use Log::Log4perl::Level;
use Moose;
use WTSI::DNAP::Utilities::Runnable;

# front-end for bcftools gtcheck function
# use to cross-check sample results in a single VCF file for consistency
# parse results and output in useful formats

our $VERSION = '';

our $MAX_DISCORDANCE_KEY = 'MAX_DISCORDANCE';
our $PAIRWISE_DISCORDANCE_KEY = 'PAIRWISE_DISCORDANCE';

with 'WTSI::DNAP::Utilities::Loggable';

has 'environment' =>
    (is       => 'ro',
     isa      => 'HashRef',
     required => 1,
     default  => sub { \%ENV });

=head2 run

  Arg [1]    : String. Either the contents of, or the path to, a VCF file.
  Arg [2]    : Boolean. True if Arg [1] is a string of VCF data, False if
               Arg [1] is the path to a file.

  Example    : ($results, $max) = $gtcheck->run($my_vcf_path, 0)
  Description: Run 'bcftools gtcheck' on given input, parse the results and
               return pairwise discordance data
  Returntype : HashRef[HashRef[Float]], Float

               First return value is reference to a hash of hashes with
               pairwise sample discordance, indexed by sample name. For
               convenience, the hash contains discordance for
               (sample_j, sample_i) as well as (sample_i, sample_j) even
               though they are the same thing.

               Second return value is the maximum pairwise discordance across
               all samples.

=cut


sub run {
    my $self = shift;
    my $input = shift;
    my $from_stdin = shift; # 1 if input is string for STDIN, 0 otherwise
    unless ($input) {
        $self->logcroak("No input supplied for bcftools gtcheck");
    }
    my $bcftools = $self->_find_bcftools();
    $self->logger->info("Running bcftools command: $bcftools");
    my (@args, @raw_results);
    if ($from_stdin) {
        unless ($self->_valid_vcf_fileformat($input)) {
            $self->logcroak("VCF input string for STDIN is not valid");
        }
        @args = ('gtcheck', '-', '-G', 1);
        @raw_results = WTSI::DNAP::Utilities::Runnable->new
            (executable  => $bcftools,
             arguments   => \@args,
             environment => $self->environment,
             logger      => $self->logger,
             stdin       => \$input)->run->split_stdout;
    } else {
        @args = ('gtcheck', $input, '-G', 1);
        @raw_results = WTSI::DNAP::Utilities::Runnable->new
            (executable  => $bcftools,
             arguments   => \@args,
             environment => $self->environment,
             logger      => $self->logger)->run->split_stdout;
    }
    $self->logger->info("bcftools arguments: ".join " ", @args );
    $self->logger->debug("bcftools command output:\n".join "", @raw_results);
    my %results;
    my $max = 0; # maximum pairwise discordance
    foreach my $line (@raw_results) {
        if ($line !~ /^CN/) { next; }
        my @words = split /\s+/, $line;
        my $discordance = $words[1];
        my $sites = $words[2];
        my $sample_i = $words[4];
        my $sample_j = $words[5];
        if (!defined($discordance)) {
            $self->logcroak("Cannot parse discordance from output: $line");
        } elsif (!defined($sites)) {
            $self->logcroak("Cannot parse sites from output: $line");
        } elsif (!($sample_i && $sample_j)) {
            $self->logcroak("Cannot parse sample names from output: $line");
        }
        my $discord_rate;
        if ($sites == 0) {
            $discord_rate = 'NA';
        } else {
            $discord_rate = $discordance / $sites; 
            if ($discord_rate > $max) { $max = $discord_rate; }
        }
        $results{$sample_i}{$sample_j} = $discord_rate;
        $results{$sample_j}{$sample_i} = $discord_rate;
    }
    return (\%results, $max);
}


=head2 run_with_file

  Arg [1]    : Path to VCF input file

  Example    : run_with_file($my_vcf_path)
  Description: Convenience method to execute 'run' on an input file
  Returntype : As for run()

=cut

sub run_with_file {
    my $self = shift;
    my $input = shift;
    if (!(-e $input)){
        $self->logcroak("Input path '$input' does not exist");
    }
    return $self->run($input, 0);
}

=head2 run_with_string

  Arg [1]    : String containing VCF input

  Example    : run_with_string($my_vcf_string)
  Description: Convenience method to execute 'run' on an input string
  Returntype : As for run()

=cut

sub run_with_string {
    my $self = shift;
    my $input = shift;
    return $self->run($input, 1);
}

=head2 write_results_json

  Arg [1]    : Hash of hashes of discordance, as returned by run()
  Arg [2]    : Maximum pairwise discordance, as returned by run()
  Arg [3]    : Path for JSON output

  Example    : write_results_json($results, $max, $my_json_path)
  Description: Write results of 'bcftools gtcheck' in JSON format
  Returntype : Int; returns 1 on successful exit

=cut

sub write_results_json {
    my $self = shift;
    my $resultsRef = shift;
    my $maxDiscord = shift;
    my $outPath = shift;
    my %output = ($MAX_DISCORDANCE_KEY => $maxDiscord,
                  $PAIRWISE_DISCORDANCE_KEY => $resultsRef);
    open my $out, '>:encoding(utf8)', $outPath || 
        $self->logcroak("Cannot open output $outPath");
    print $out encode_json(\%output);
    close $out || $self->logcroak("Cannot open output $outPath");
    return 1;
}

=head2 write_results_text

  Arg [1]    : Hash of hashes of discordance, as returned by run()
  Arg [2]    : Maximum pairwise discordance, as returned by run()
  Arg [3]    : Path for text output, or - for STDOUT

  Example    : write_results_text($results, $max, $my_text_path)
  Description: Write results of 'bcftools gtcheck' in tab-delimited text
               format. Maximum pairwise discordance appears in header, each
               line in body consists of sample names and pairwise discordance
               (rounded to 5 decimal places).
  Returntype : Int; returns 1 on successful exit

=cut

sub write_results_text {
    my $self = shift;
    my %results = %{ shift() };
    my $maxDiscord = shift;
    my $outPath = shift;
    my @samples = sort(keys(%results));
    my $out;
    if ($outPath eq '-') {
        $out = *STDOUT;
    } else {
        open $out, '>:encoding(utf8)', $outPath || 
            $self->logcroak("Cannot open output $outPath");
    }
    printf $out "# $MAX_DISCORDANCE_KEY: %.5f\n", $maxDiscord;
    print $out "# sample_i\tsample_j\tpairwise_discordance\n";
    foreach my $sample_i (@samples) {
        foreach my $sample_j (@samples) {
            if ($sample_i eq $sample_j) { next; }
            my @fields = ($sample_i,
                          $sample_j,
                          $results{$sample_i}{$sample_j});
            if ($fields[2] eq 'NA') {
                printf $out "%s\t%s\t%s\n", @fields;
            } else {
                printf $out "%s\t%s\t%.5f\n", @fields;
            }
        }
    }
    if ($outPath ne '-') {
        close $out || $self->logcroak("Cannot close output $outPath");
    }
    return 1;
}

sub _find_bcftools {
    # check existence and version of the bcftools executable
    my $self = shift;
    my @raw_results = WTSI::DNAP::Utilities::Runnable->new
        (executable  => 'which',
         arguments   => ['bcftools',],
         environment => $self->environment,
         logger      => $self->logger)->run->split_stdout;
    my $bcftools = shift @raw_results;
    chomp $bcftools;
    if (!$bcftools) { $self->logcroak("Cannot find bcftools executable"); }
    @raw_results = WTSI::DNAP::Utilities::Runnable->new
        (executable  => 'bcftools',
         arguments   => ['--version',],
         environment => $self->environment,
         logger      => $self->logger)->run->split_stdout;
    my $version_string = shift @raw_results;
    chomp $version_string;
    if ($version_string =~ /^bcftools 0\.[01]\./ ||
            $version_string =~ /^bcftools 0\.2\.0-rc[12345678]$/) {
        $self->logger->logwarn("Must have bcftools version >= 0.2.0-rc9");
    }
    return $bcftools;
}

sub _valid_vcf_fileformat {
    # Check if VCF string starts with a valid fileformat specifier
    # Eg. ##fileformat=VCFv4.1
    # Intended as a simple sanity check; does not validate rest of VCF
    my $self = shift;
    my $input = shift;
    my $valid = $input =~ /^##fileformat=VCFv[0-9]+\.[0-9]+/;
    return $valid;
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::GtcheckWrapper

=head1 DESCRIPTION

A class to run 'bcftools gtcheck', to measure concordance of genotype calls
between samples in a VCF file. Requires bcftools version >= 0.2.0-rc9.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
