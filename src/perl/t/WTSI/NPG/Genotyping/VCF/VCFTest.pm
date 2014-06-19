package WTSI::NPG::Genotyping::VCFTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use File::Temp qw(tempdir);
use JSON;
use Test::More tests => 18;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $tmp;

BEGIN {
    use_ok('WTSI::NPG::Genotyping::VCF::VCFConverter');
    use_ok('WTSI::NPG::Genotyping::VCF::VCFGtcheck');
}

use WTSI::NPG::Genotyping::VCF::VCFConverter;
use WTSI::NPG::Genotyping::VCF::VCFGtcheck;

my $data_path = './t/vcf';
my $irods_tmp_coll;
my $pid = $$;

sub setup: Test(setup) {
    $tmp = tempdir("vcftest_XXXXXX", CLEANUP => 1);
    my $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = "VCFTest.$pid";
    $irods->add_collection($irods_tmp_coll);
    $irods_tmp_coll = $irods->absolute_path($irods_tmp_coll);
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub fluidigm_test : Test(6) {
    # upload test data to temporary irods collection
    my (@inputs, $converter, $output);
    @inputs = _upload_fluidigm();
    $converter = WTSI::NPG::Genotyping::VCF::VCFConverter->new(inputs => \@inputs, verbose => 0, input_type => 'fluidigm', 'fluidigm_plex_dir' => $irods_tmp_coll);
    my $vcf = $tmp.'/conversion_test_fluidigm.vcf';
    ok($converter->convert($vcf), "Converted Fluidigm results to VCF");
    my $gtcheck = WTSI::NPG::Genotyping::VCF::VCFGtcheck->new(input => $vcf, verbose => 0);
    my ($resultRef, $maxDiscord) = $gtcheck->run();
    ok($resultRef, "Gtcheck result");
    is(scalar(keys(%{$resultRef})), 4, "Correct number of samples in result");
    my $outputJson = $tmp."/pairwise_discordance.json";
    my $outputText = $tmp."/pairwise_discordance.txt";
    ok($gtcheck->write_results_json($resultRef, $maxDiscord, $outputJson), "JSON output written");
    ok($gtcheck->write_results_text($resultRef, $maxDiscord, $outputText), "Text output written");
    my $refJsonPath = "$data_path/pairwise_discordance_fluidigm.json";
    open my $in, "<", $refJsonPath || log->logcroak("Cannot open input $refJsonPath");
    my $refJson = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $refJsonPath");
    open $in, "<", $outputJson || log->logcroak("Cannot open input $outputJson");
    my $outJson = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $outputJson");
    is_deeply($outJson, $refJson, "Output and expected data structures match");
}

sub sequenom_test : Test(6) {
    # upload test data to temporary irods collection
    my (@inputs, $converter, $output);
    @inputs = _upload_sequenom();
    $converter = WTSI::NPG::Genotyping::VCF::VCFConverter->new(inputs => \@inputs, verbose => 0, input_type => 'sequenom', 'sequenom_plex_dir' => $irods_tmp_coll);
    my $vcf = $tmp.'/conversion_test_sequenom.vcf';
    ok($converter->convert($vcf), "Converted Sequenom results to VCF");
    my $gtcheck = WTSI::NPG::Genotyping::VCF::VCFGtcheck->new(input => $vcf, verbose => 0);
    my ($resultRef, $maxDiscord) = $gtcheck->run();
    ok($resultRef, "Gtcheck result");
    is(scalar(keys(%{$resultRef})), 4, "Correct number of samples in result");
    my $outputJson = $tmp."/pairwise_discordance.json";
    my $outputText = $tmp."/pairwise_discordance.txt";
    ok($gtcheck->write_results_json($resultRef, $maxDiscord, $outputJson), "JSON output written");
    ok($gtcheck->write_results_text($resultRef, $maxDiscord, $outputText), "Text output written");
    my $refJsonPath = "$data_path/pairwise_discordance_sequenom.json";
    open my $in, "<", $refJsonPath || log->logcroak("Cannot open input $refJsonPath");
    my $refJson = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $refJsonPath");
    open $in, "<", $outputJson || log->logcroak("Cannot open input $outputJson");
    my $outJson = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $outputJson");
    is_deeply($outJson, $refJson, "Output and expected data structures match");
}

sub script_test : Test(4) {
    # simple test of command-line script
    my $script = 'bin/vcf_from_plex.pl';
    # input list contains paths in temp irods collection, need to write it on the fly
    my @inputs = _upload_sequenom();
    my $sequenomList = "$tmp/sequenom_inputs.txt";
    open my $out, ">", $sequenomList || log->logcroak("Cannot open output $sequenomList");
    print $out join("\n", @inputs)."\n";
    close $out || log->logcroak("Cannot close output $sequenomList");
    my $tmpJson = "$tmp/sequenom.json";
    my $tmpText = "$tmp/sequenom.txt";
    my $cmd = "$script --input - --plex_type sequenom --text $tmpText --json $tmpJson  < $sequenomList";
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $tmpText, "text output written");
    ok(-e $tmpJson, "JSON output written");
    my $refJsonPath = "$data_path/pairwise_discordance_sequenom.json";
    open my $in, "<", $refJsonPath || log->logcroak("Cannot open input $refJsonPath");
    my $refJson = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $refJsonPath");
    open $in, "<", $tmpJson || log->logcroak("Cannot open input $tmpJson");
    my $outJson = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $tmpJson");
    is_deeply($outJson, $refJson, "Output and expected data structures match");
}

sub _upload_fluidigm {
    my $irods = WTSI::NPG::iRODS->new;
    my @csv_files = qw/fluidigm_001.csv fluidigm_002.csv
                       fluidigm_003.csv fluidigm_004.csv/;
    my $manifest = "$data_path/qc_fluidigm_snp_info_GRCh37.tsv";
    return _upload_plex_files(\@csv_files, $manifest, 'fluidigm_plex', 'qc');
}

sub _upload_sequenom {
    my @csv_files = qw/sequenom_001.csv sequenom_002.csv
                       sequenom_003.csv sequenom_004.csv/;
    my $manifest = "$data_path/W30467_snp_set_info_GRCh37.tsv";
    return _upload_plex_files(\@csv_files, $manifest, 'sequenom_plex', 'W30467');
}

sub _upload_plex_files {
    # upload plex data to the test irods
    # return list of uploaded CSV paths, *not* including the manifest
    my $irods = WTSI::NPG::iRODS->new;
    my @csv_files = @{ shift() };
    my $manifest = shift;
    my $manifest_key = shift;
    my $manifest_value = shift;
    my @inputs;
    foreach my $csv (@csv_files) {
        my $ipath = "$irods_tmp_coll/$csv";
        push(@inputs, $ipath);
        $irods->add_object("$data_path/$csv", $ipath);
        my $csv_obj = WTSI::NPG::iRODS::DataObject->new
            ($irods, "$irods_tmp_coll/$csv" )->absolute;
        $csv_obj->add_avu($manifest_key, $manifest_value);
    }
    $irods->add_object($manifest, $irods_tmp_coll);
    return @inputs;
}


return 1;
