#! /software/bin/perl

use utf8;

package main;

use warnings;
use strict;

use FindBin qw($Bin);
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator;
use WTSI::NPG::Genotyping::QC_wip::Check::SampleIdentityBayesian;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Utilities qw(user_session_log);

# script to explore effect of varying parameters in Bayesian ID check
# previously in separate Git repository as vary_qc_data.pl
# used to generate input for R plots of parameter effects

# inputs: simulation mode, (range of) model params
# outputs: tab-separated (parameter, concordance, identity) triples

our $VERSION = '';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'check_identity_bed_wip');

my $embedded_conf = "
   log4perl.logger.npg.genotyping.qc.identity = ERROR, A1, A2

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

    my $debug;
    my $log4perl_config;
    my $mode;
    my $snpset_file;
    my $verbose;
    my $start;
    my $incr;
    my $total;

    my $mode_vary_ecp = 'ecp'; # equivalent calls probability
    my $mode_vary_smp = 'smp'; # sample mismatch prior
    my $mode_vary_xer = 'xer'; # expected error rate
    my $mode_vary_qcs = 'qcs'; # qc SNPs
    my $mode_vary_qcr = 'qcr'; # qc runs

    my @modes = ($mode_vary_ecp,
                 $mode_vary_smp,
                 $mode_vary_xer,
                 $mode_vary_qcs,
                 $mode_vary_qcr);

    GetOptions(
        'debug'             => \$debug,
        'help'              => sub { pod2usage(-verbose => 2,
                                               -exitval => 0) },
        'incr=f'            => \$incr,
        'logconf=s'         => \$log4perl_config,
        'mode=s'            => \$mode,
        'snpset=s'          => \$snpset_file,
        'start=f'           => \$start,
        'total=i'           => \$total,
        'verbose'           => \$verbose);

    if ($log4perl_config) {
        Log::Log4perl::init($log4perl_config);
        $log = Log::Log4perl->get_logger('npg.genotyping.qc.identity');
    }
    else {
        Log::Log4perl::init(\$embedded_conf);
        $log = Log::Log4perl->get_logger('npg.genotyping.qc.identity');
        if ($verbose) {
            $log->level($INFO);
        }
        elsif ($debug) {
            $log->level($DEBUG);
        }
    }

    my $data_path = $Bin.'/../t/qc/check/identity';
    $snpset_file ||= "$data_path/W30467_snp_set_info_1000Genomes.tsv";
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    my $calls = generate_calls($snpset);

    my $idsim = WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator->new(
        calls  => $calls,
        snpset => $snpset,
        logger => $log,
    );

    my $results;

    # either all range options, or none of them, may have arguments
    my $range_ok = 0;
    if (defined($start) && defined($incr) && defined($total)) {
        $range_ok = 1;
    } elsif (!(defined($start) || defined($incr) || defined($total))) {
        $range_ok = 1;
    }
    unless ($range_ok) {
        $log->logcroak("Invalid range: Must supply arguments for all of ",
                       "(--start, --incr, --total), or none of them.");
    }

    if (!defined($mode)) {
        $log->logcroak("Mode argument is required");
    } elsif ($mode eq $mode_vary_ecp) {
        $results = $idsim->find_identity_vary_ecp($start, $incr, $total);
    } elsif ($mode eq $mode_vary_qcr) {
        $results = $idsim->find_identity_vary_qcr($start, $incr, $total);
    } elsif ($mode eq $mode_vary_qcs) {
        $results = $idsim->find_identity_vary_qcs($start, $incr, $total);
    } elsif ($mode eq $mode_vary_smp) {
        $results = $idsim->find_identity_vary_smp($start, $incr, $total);
    } elsif ($mode eq $mode_vary_xer) {
        $results = $idsim->find_identity_vary_xer($start, $incr, $total);
    } else {
        $log->logcroak("Illegal mode argument '", $mode,
                       "'; permitted values are: (",
                       join(', ', @modes),
                       "). Run with --help for details.");
    }
    print $mode."\tconcord\tid\n";
    my $format;
    if ($mode eq $mode_vary_qcs || $mode eq $mode_vary_qcr) {
        $format = "%d\t%.3f\t%.8f\n"; # integer parameter
    } else {
        $format = "%.3f\t%.3f\t%.8f\n"; # float parameter
    }

    foreach my $result (@{$results}) {
        printf $format, @{$result};
    }
}


sub generate_calls {
    my ($snpset, ) = @_;
    # snpset argument must include the snps in hard-coded calls below
    # full set of production data
    # 25 SNPs (excluding gender markers)
    # TODO flexibly generate fake calls from the snpset, with a given het rate
    my @data = (
        ['rs649058',   'AG'],
        ['rs1131498',  'AA'],
        ['rs1805087',  'AG'],
        ['rs3795677',  'AG'],
        ['rs6166',     'AG'],
        ['rs1801262',  'AA'],
        ['rs2286963',  'GT'],
        ['rs6759892',  'GT'],
        ['rs7627615',  'AG'],
        ['rs11096957', 'AA'],
        ['rs2247870',  'CT'],
        ['rs4619',     'AG'],
        ['rs532841',   'CT'],
        ['rs6557634',  'CT'],
        ['rs4925',     'AC'],
        ['rs156697',   'AA'],
        ['rs5215',     'CT'],
        ['rs12828016', 'AA'],
        ['rs7298565',  'AG'],
        ['rs3742207',  'AC'],
        ['rs4075254',  'CT'],
        ['rs4843075',  'GA'],
        ['rs8065080',  'CT'],
        ['rs1805034',  'AA'],
        ['rs2241714',  'CT'],
        ['rs753381',   'AG']
    );

    my @calls = map {
        my ($snp, $genotype) = @$_;
        WTSI::NPG::Genotyping::Call->new
              (snp      => $snpset->named_snp($snp),
               genotype => $genotype) } @data;
    return \@calls;
}

# TODO could use area under curve as a summary statistic


__END__

=head1 NAME

identity_simulation

=head1 SYNOPSIS

identity_simulation --mode NUM [--help] [--snpset PATH] [--verbose]

Options:

  --help                 Display help.
  --incr=NUM             Increment for parameter values. Optional.
  --logconf=PATH         Path to Perl logger configuration file. Optional.
  --mode=STRING          String to identify the simulation mode, ie. the
                         parameter to be varied. See below for list of
                         permitted modes.
  --snpset=PATH          Path to .tsv snpset manifest file. Optional,
                         defaults to copy of W30467 manifest in local test
                         directory.
  --start=NUM            Starting value for parameter to be varied. Optional.
  --total=INT            Total number of parameter values. Optional.
  --verbose              Turn on verbose logging. Optional.

=head1 DESCRIPTION

Generate simulated data for the Bayesian identity check and evaluate the
identity metric over a range of concordance. Tab-delimited results are
written to standard output.

If given, the --start, --incr, and --total arguments control the range of
parameters to be simulated. For example, with --start 0, --incr 0.2,
--total 4, the parameter values will be (0.0, 0.2, 0.4, 0.6). If only one
or two of --start, --incr, and --total are given, an error is thrown. If
none are given, appropriate default values will be used. Run with the
--verbose option to view the parameter range in use. The script will throw
an error if an inappropriate range is chosen (eg. a probability greater
than 1).

The --mode argument is a three-letter code identifying which parameter
will be varied, as follows:

=over

=item * ecp: Equivalent Calls Probability. Probability of equivalent genotype calls on unrelated samples.

=item * qcs: Quality Control SNPs. Total number of SNPs in QC set.

=item * qcr: Quality Control Runs. Total number of (identical) quality control runs, ie. number of QC calls for each SNP and sample.

=item * smp: Sample Mismatch Prior. The Bayesian prior probability of non-identical samples.

=item * xer: Expected Error Rate. The probability of non-equivalent calls on the same sample. This is a proxy for the calling error rate.

=back

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
