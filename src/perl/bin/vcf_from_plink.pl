#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use File::Slurp qw(read_file);
use Getopt::Long;
use JSON;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;
use Text::CSV;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::VCF::DataRow;
use WTSI::NPG::Genotyping::VCF::Header;
use WTSI::NPG::Genotyping::VCF::VCFDataSet;
use WTSI::NPG::Utilities qw(user_session_log);

# script to read (non-binary) Plink data and write as VCF

our $VERSION = '';


my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'check_identity_bed_wip');

my $embedded_conf = "
   log4perl.logger.npg.genotyping.vcf_from_plink = ERROR, A1, A2

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2           = Log::Log4perl::Appender::File
   log4perl.appender.A2.filename  = $session_log
   log4perl.appender.A2.utf8      = 1
   log4perl.appender.A2.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.syswrite  = 1
";

my $log;

run() unless caller();

sub run {

    my $contigs;
    my $debug;
    my $log4perl_config;
    my $manifest;
    my $plink;
    my $vcf;
    my $verbose;

    # required arguments: plink in, vcf out
    GetOptions(
        'contigs=s'         => \$contigs,
        'debug'             => \$debug,
        'help'              => sub { pod2usage(-verbose => 2,
                                               -exitval => 0) },
        'logconf=s'         => \$log4perl_config,
        'manifest=s'        => \$manifest,
        'plink=s'           => \$plink,
        'vcf=s'             => \$vcf,
        'verbose'           => \$verbose,
    );

    if ($log4perl_config) {
        Log::Log4perl::init($log4perl_config);
        $log = Log::Log4perl->get_logger('npg.genotyping.vcf_from_plink');
    }
    else {
        Log::Log4perl::init(\$embedded_conf);
        $log = Log::Log4perl->get_logger('npg.genotyping.vcf_from_plink');
        if ($verbose) {
            $log->level($INFO);
        }
        elsif ($debug) {
            $log->level($DEBUG);
        }
    }

    unless ($contigs && $manifest && $plink && $vcf) {
        $log->logcroak("Missing required argument: Must supply --contigs, ",
                       "--manifest, --plink, and --vcf");
    }

    # read calls in sample-major order from Plink
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($manifest);
    my $mapPath = $plink.".map";
    my $pedPath = $plink.".ped";
    my $plink_snp_names = read_plink_snp_names($mapPath);
    my @plink_snps;
    foreach my $name (@{$plink_snp_names}) {
        my $snp = $snpset->named_snp($name);
        if (defined($snp)) {
            push @plink_snps, $snp;
        } else {
            croak("SNP named '$name' is not present in manifest");
        }
    }
    my $sample_names = read_column($pedPath, 1, ' '); # preserves sample order
    my $calls_by_sample = read_plink_calls($pedPath, \@plink_snps);

    # transpose to snp-major order and create VCF::DataRow objects
    my @dataRows = ();
    for (my $i=0;$i<@plink_snps;$i++) {
        my @snp_calls;
        foreach my $sample_name (@{$sample_names}) {
            my @sample_calls = @{$calls_by_sample->{$sample_name}};
            push @snp_calls, $sample_calls[$i];
        }
        my $dataRow = WTSI::NPG::Genotyping::VCF::DataRow->new(
            calls => \@snp_calls,
        );
        push @dataRows, $dataRow;
    }

    # create VCF::Header
    my $contig_lengths = decode_json(read_file($contigs));
    my $header = WTSI::NPG::Genotyping::VCF::Header->new(
        sample_names   => $sample_names,
        contig_lengths => $contig_lengths,
        reference      => $manifest,
    );

    # create VCF::VCFDataSet and write object
    my $dataset = WTSI::NPG::Genotyping::VCF::VCFDataSet->new(
        header => $header,
        data   => \@dataRows,
    );
    open my $out, ">", $vcf || croak("Cannot open VCF output '$vcf'");
    print $out $dataset->str();
    close $out || croak("Cannot close VCF output '$vcf'");

}

sub read_plink_calls {
    # want to create a list of Call objects for each sample name
    # return a HashRef of calls; transpose the hashref for VCF output
    my ($pedPath, $plink_snps_ref) = @_;
    my @plink_snps = @{$plink_snps_ref};
    my @ped_lines = read_file($pedPath);
    my $csv = Text::CSV->new({sep_char => " "});
    my %calls_by_sample;
    foreach my $line (@ped_lines) {
        $csv->parse($line);
        my @fields = $csv->fields();
        my $sample_name = $fields[1];
        my @calls;
        my $i = 0;
        while ($i < @plink_snps) {
            my $snp = $plink_snps[$i];
            my $gt = $fields[2*$i+6].$fields[2*$i+7];
            my $call;
            if ($gt eq '00') {
                $call = WTSI::NPG::Genotyping::Call->new(
                    snp      => $snp,
                    genotype => 'NN',
                    is_call  => 0,
                );
            } else {
                $call = WTSI::NPG::Genotyping::Call->new(
                    snp      => $snp,
                    genotype => $gt,
                    is_call  => 1,
                );
            }
            push @calls, $call;
            $i += 1;
        }
        $calls_by_sample{$sample_name} = \@calls;
    }
    return \%calls_by_sample;
}

sub read_plink_snp_names {
    my ($mapPath,) = @_;
    my @names;
    my $names_raw = read_column($mapPath, 1);
    foreach my $name_raw (@{$names_raw}) {
        my @id = split /-/, $name_raw;
        my $name = pop @id;
        push @names, $name;
    }
    return \@names;
}

sub read_column {
    # read a given column from a .tsv file
    my ($inPath, $index, $sep_char) = @_;
    $sep_char ||= "\t";
    my @values;
    my @inLines = read_file($inPath);
    my $csv = Text::CSV->new({sep_char => $sep_char});
    foreach my $line (@inLines) {
        $csv->parse($line);
        my @fields = $csv->fields();
        push @values, $fields[$index];
    }
    return \@values;
}


__END__

=head1 NAME

vcf_from_plink

=head1 SYNOPSIS

vcf_from_plink.pl --contig <contig JSON file> --out <path>
--plex-manifest <path> --plink <path stem> --vcf <VCF file> [--logconf PATH]
[--help] [--verbose]

Options:


  --contig=PATH          Path to JSON file with contig lengths. Same format
                         as expected by vcf_from_plex.pl.
  --help                 Display help.
  --logconf=PATH         Path to Perl logger configuration file. Optional.
  --manifest=PATH   Path to .csv SNP manifest. The manifest must
                         contain all SNPs present in the Plink .map file.
                         Required.
  --plink=STEM           Plink text dataset stem (path omitting the .ped,
                         .map suffix) for production data.
  --vcf=PATH             Path to VCF output file. Required.
  --verbose              Print messages while processing. Optional.

=head1 DESCRIPTION

Convenience script to convert a Plink text dataset to VCF.

The script only supports non-binary Plink data; a binary dataset
(consisting of .bed, .bim, .fam files) can be converted to non-binary format
using the Plink application.

=head1 METHODS

None

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
