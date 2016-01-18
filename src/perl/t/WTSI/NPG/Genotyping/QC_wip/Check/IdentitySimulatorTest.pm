
package WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulatorTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Temp qw(tempdir);
use Test::More tests => 18;
use Test::Exception;
use Text::CSV;

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator;
use WTSI::NPG::Genotyping::SNPSet;

Log::Log4perl::init('./etc/log4perl_tests.conf');
my $log = Log::Log4perl->get_logger();

my $data_path = './t/qc/check/identity';
my $snpset_file = "$data_path/W30467_snp_set_info_1000Genomes.tsv";
my $snpset;
my $calls;

sub setup : Test(setup) {

    $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_file);

    # copy-pasted from identity_simulation.pl
    # useful to have a fixed set of test calls
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
    $calls = \@calls;

    my $id_sim = WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator->new(
        calls  => \@calls,
        snpset => $snpset
    );
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator');
}


sub construct : Test(1) {

    my @args = (snpset           => $snpset,
                calls            => $calls);
    new_ok('WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator'
               => \@args);
}

sub script : Test(11) {

    my $script = "./bin/identity_simulation.pl";
    my $tempdir = tempdir("id_sim_test_XXXXXX", cleanup => 1);
    my @modes = qw/ecp qcs qcr smp xer/;
    foreach my $mode (@modes) {
        my $output = "$tempdir/$mode.txt";
        my $cmd = "$script --mode $mode > $output";
        is(system($cmd), 0, "$cmd executed successfully");
        my $results = _read_tsv($output);
        my $expected =  _read_tsv($data_path."/simulated_$mode.txt");
        is_deeply($results, $expected,
                  uc($mode)." results match expected values");
    }
    my $cmd = "$script --mode foo &> /dev/null";
    isnt(system($cmd), 0, "Fails with invalid mode argument");
}

sub simulate : Test(5) {

    my $id_sim = WTSI::NPG::Genotyping::QC_wip::Check::IdentitySimulator->new(
        snpset           => $snpset,
        calls            => $calls);
    my $results;
    $results = $id_sim->find_identity_vary_ecp(0, 0.2, 5);
    is(scalar @{$results}, 135, "Correct number of ECP results");
    $results = $id_sim->find_identity_vary_qcr(1, 1, 2);
    is(scalar @{$results}, 54, "Correct number of QCR results");
    $results = $id_sim->find_identity_vary_qcs(4, 10, 2);
    is(scalar @{$results}, 20, "Correct number of QCS results");
    $results = $id_sim->find_identity_vary_smp(0.1, 0.1, 4);
    is(scalar @{$results}, 108, "Correct number of SMP results");
    $results = $id_sim->find_identity_vary_xer(0.05, 0.05, 4);
    is(scalar @{$results}, 108, "Correct number of XER results");
}

sub _read_tsv {
    # read a tab-delimited file
    my ($path, ) = @_;
    my $csv = Text::CSV->new({sep_char => "\t"});
    my @results;
    open my $in, "<", $path || $log->logcroak("Cannot open '$path'");
    while (my $row = $csv->getline($in)) {
        push @results, $row;
    }
    close $in || $log->logcroak("Cannot close '$path'");
    return \@results;
}

1;
