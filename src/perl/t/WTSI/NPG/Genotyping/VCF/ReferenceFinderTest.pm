package WTSI::NPG::Genotyping::VCF::ReferenceFinderTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);

use Test::More tests => 5;

use File::Path qw/make_path/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw/tempdir/;Log::Log4perl::init('./etc/log4perl_tests.conf');
use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $log = Log::Log4perl->get_logger();

my $reference_genome = 'Homo_sapiens (GRCh37_53)';
my $reference_file_name = 'Homo_sapiens.GRCh37.dna.all.fa';
my $reference_file_path;
my $repository;
my $repository2;

BEGIN {
    use_ok('WTSI::NPG::Genotyping::VCF::ReferenceFinder');
}

use WTSI::NPG::Genotyping::VCF::ReferenceFinder;

sub setup: Test(setup) {
    # repository root path must have a subdirectory called 'references'
    $repository = tempdir(CLEANUP => 1);
    my $fastadir = catfile($repository, 'references', 'Homo_sapiens',
                           'GRCh37_53', 'all', 'fasta');
    make_path($fastadir);
    $reference_file_path = catfile($fastadir,
                                   'Homo_sapiens.GRCh37.dna.all.fa');
    # create an empty FASTA reference file
    # npg_tracking::data::reference::list::ref_file_prefix expects this
    # file to exist for *any* 'aligner' value (not just 'fasta')
    _write_empty_file($reference_file_path);
}

sub finder_test : Test(3) {
    new_ok('WTSI::NPG::Genotyping::VCF::ReferenceFinder',
           [ reference_genome => $reference_genome,
             repository => $repository, ]);
    my $finder = WTSI::NPG::Genotyping::VCF::ReferenceFinder->new(
        reference_genome => $reference_genome,
        repository => $repository,
    );
    my $got_ref = $finder->reference_path;
    is($got_ref, $reference_file_path,
       "Reference path for '".$reference_genome."' matches expected value");
    my $got_uri = $finder->get_reference_uri();
    my $expected_uri = 'file://'.$reference_file_path;
    is($got_uri, $expected_uri,
       "URI for '".$reference_genome."' matches expected value");
}

sub whitespace_uri_test : Test(1) {
    # create an alternate repository with whitespace in path
    my $repository_2 = catfile($repository, 'my references');
    my $fastadir_2 = catfile($repository_2, 'references', 'Homo_sapiens',
                             'GRCh37_53', 'all', 'fasta');
    make_path($fastadir_2);
    my $reference_file_path_2 = catfile($fastadir_2,
                                        'Homo_sapiens.GRCh37.dna.all.fa');
    _write_empty_file($reference_file_path_2);
    my $finder = WTSI::NPG::Genotyping::VCF::ReferenceFinder->new(
        reference_genome => $reference_genome,
        repository => $repository_2,
    );
    my $got_uri = $finder->get_reference_uri();
    my $expected_uri = 'file://'.$repository.'/my%20references/references'.
        '/Homo_sapiens/GRCh37_53/all/fasta/'.$reference_file_name;
    is($got_uri, $expected_uri, "Path with whitespace matches expected URI");
}

sub _write_empty_file {
    my ($path) = @_;
    open my $fh, '>>', $path || $log->logcroak(
        "Cannot open file at '", $path, "'");
    close $fh || $log->logcroak(
        "Cannot close file at '", $path, "'");
}

return 1;
