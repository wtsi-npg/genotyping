package WTSI::NPG::Genotyping::VCFTest;

use strict;
use warnings;

use base qw(Test::Class);
use Cwd qw(abs_path);
use File::Spec;
use File::Temp qw(tempdir);
use JSON;
use Test::More tests => 44;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $tmp;
our @fluidigm_csv = qw/fluidigm_001.csv fluidigm_002.csv
                       fluidigm_003.csv fluidigm_004.csv/;
our @sequenom_csv = qw/sequenom_001.csv sequenom_002.csv
                       sequenom_003.csv sequenom_004.csv/;

our $chromosome_lengths;

our $SEQUENOM_TYPE = 'sequenom'; # TODO avoid repeating these across modules
our $FLUIDIGM_TYPE = 'fluidigm';

BEGIN {
    use_ok('WTSI::NPG::Genotyping::VCF::AssayResultReader');
    use_ok('WTSI::NPG::Genotyping::VCF::DataRow');
    use_ok('WTSI::NPG::Genotyping::VCF::Header');
    use_ok('WTSI::NPG::Genotyping::VCF::GtcheckWrapper');
    use_ok('WTSI::NPG::Genotyping::VCF::VCFDataSet');
}

use WTSI::NPG::Genotyping::VCF::AssayResultReader;
use WTSI::NPG::Genotyping::VCF::DataRow;
use WTSI::NPG::Genotyping::VCF::GtcheckWrapper;

my $data_path = './t/vcf';
my $sequenom_snpset_name = 'W30467_snp_set_info_GRCh37.tsv';
my $sequenom_snpset_path = $data_path.'/'.$sequenom_snpset_name;
my $fluidigm_snpset_path = $data_path."/qc_fluidigm_snp_info_GRCh37.tsv";
my $chromosome_json_name = 'chromosome_lengths_GRCh37.json';
my $chromosome_json_path = $data_path.'/'.$chromosome_json_name;
my $discordance_fluidigm = $data_path."/pairwise_discordance_fluidigm.json";
my $discordance_sequenom = $data_path."/pairwise_discordance_sequenom.json";
my $vcf_fluidigm = $data_path."/fluidigm.vcf";
my $vcf_sequenom = $data_path."/sequenom.vcf";

my $irods;
my $irods_tmp_coll;
my $pid = $$;
my $testnum = 0; # use to create distinct temporary irods collections

sub setup: Test(setup) {
    $tmp = tempdir("vcftest_XXXXXX", CLEANUP => 1);
    $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = "VCFTest.$pid.$testnum";
    $testnum++;
    $irods->add_collection($irods_tmp_coll);
    $irods_tmp_coll = $irods->absolute_path($irods_tmp_coll);
    # read chromosome lengths
    $chromosome_lengths = _read_json($chromosome_json_path);
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}

sub fluidigm_file_test : Test(7) {
    my @inputs;
    foreach my $name (@fluidigm_csv) {
        push(@inputs, abs_path($data_path."/".$name));
    }
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($fluidigm_snpset_path);
    my $reader = WTSI::NPG::Genotyping::VCF::AssayResultReader->new
        (inputs => \@inputs,
         input_type => $FLUIDIGM_TYPE,
         snpset => $snpset,
         chromosome_lengths => $chromosome_lengths);
    my $vcf_dataset = $reader->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_fluidigm.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Fluidigm results to VCF with input from file");
    _test_vcf_output($vcf_fluidigm, $vcf_file);
    _test_fluidigm_gtcheck($vcf_file);
}

sub fluidigm_irods_test : Test(7) {
    # upload test data to temporary irods collection
    my @inputs = _upload_fluidigm();
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($fluidigm_snpset_path);
    my $reader = WTSI::NPG::Genotyping::VCF::AssayResultReader->new
        (inputs => \@inputs,
         irods => $irods,
         input_type => $FLUIDIGM_TYPE,
         snpset => $snpset,
         chromosome_lengths => $chromosome_lengths,
         );
    my $vcf_dataset = $reader->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_fluidigm.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Fluidigm results to VCF with input from iRODS");
    _test_vcf_output($vcf_fluidigm, $vcf_file);
    _test_fluidigm_gtcheck($vcf_file);
}

sub sequenom_file_test : Test(7) {
    # sequenom test with input from local filesystem
    my @inputs;
    foreach my $name (@sequenom_csv) {
        push(@inputs, abs_path($data_path."/".$name));
    }
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($sequenom_snpset_path);
    my $reader = WTSI::NPG::Genotyping::VCF::AssayResultReader->new
        (inputs => \@inputs,
         input_type => $SEQUENOM_TYPE,
         snpset => $snpset,
         chromosome_lengths => $chromosome_lengths);
    my $vcf_dataset = $reader->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_sequenom.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Sequenom results to VCF with input from file");
    _test_vcf_output($vcf_sequenom, $vcf_file);
    _test_sequenom_gtcheck($vcf_file);
}

sub sequenom_irods_test : Test(7) {
    # upload test data to temporary irods collection
    my @inputs = _upload_sequenom();
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($sequenom_snpset_path);
    my $reader = WTSI::NPG::Genotyping::VCF::AssayResultReader->new
        (inputs => \@inputs,
         irods => $irods,
         input_type => $SEQUENOM_TYPE,
         snpset => $snpset,
         chromosome_lengths => $chromosome_lengths,
         );
    my $vcf_dataset = $reader->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_sequenom.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Sequenom results to VCF with input from iRODS");
    _test_vcf_output($vcf_sequenom, $vcf_file);
    _test_sequenom_gtcheck($vcf_file);
}

sub script_conversion_test : Test(3) {
    # simple test of command-line script for conversion to VCF
    # input is in iRODS
    my $script = 'bin/vcf_from_plex.pl';
    my $irods = WTSI::NPG::iRODS->new;
    my @inputs = _upload_sequenom(); # write list of iRODS inputs
    my $sequenomList = "$tmp/sequenom_inputs.txt";
    open my $out, ">", $sequenomList ||
        log->logcroak("Cannot open output $sequenomList");
    foreach my $input (@inputs) { print $out $input."\n"; }
    close $out || log->logcroak("Cannot close output $sequenomList");
    my $tmpJson = "$tmp/sequenom.json";
    my $tmpText = "$tmp/sequenom.txt";
    my $vcfOutput = "$tmp/vcf.txt";
    my $snpset_ipath = $irods_tmp_coll.'/'.$sequenom_snpset_name;
    my $cmd = "$script --input - --vcf $vcfOutput  --quiet ".
        "--snpset $snpset_ipath --irods --plex_type $SEQUENOM_TYPE ".
        "< $sequenomList";
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $vcfOutput, "VCF output written");
    # read VCF output (omitting date) and compare to reference file
    my @buffer = ();
    _test_vcf_output($vcf_sequenom, $vcfOutput);
}

sub script_gtcheck_test : Test(4) {
    # simple test of command-line script for genotype consistency check
    my $script = 'bin/vcf_consistency_check.pl';
    my $vcf = "$data_path/sequenom.vcf";
    my $tmpJson = "$tmp/sequenom.json";
    my $tmpText = "$tmp/sequenom.txt";
    my $cmd = "$script --input - --text - --json $tmpJson < $vcf > $tmpText";
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $tmpText, "text output written");
    ok(-e $tmpJson, "JSON output written");
    _compare_json($discordance_sequenom, $tmpJson);
}

sub script_pipe_test : Test(4) {
    # test with output of one VCF script piped into the other
    # initial input is in local filesystem (not iRODS)
    my $converter = 'bin/vcf_from_plex.pl';
    my $checker = 'bin/vcf_consistency_check.pl';
    my $sequenomList = "$data_path/sequenom_inputs.txt";
    my $tmpJson = "$tmp/sequenom.json";
    my $tmpText = "$tmp/sequenom.txt";
    my @cmds = (
        "cat $sequenomList",
        "$converter --input - --vcf - --snpset $sequenom_snpset_path ".
            "--quiet --chromosomes $chromosome_json_path --plex_type sequenom",
        "$checker --input - --text $tmpText --json $tmpJson"
    );
    my $cmd = join(' | ', @cmds);
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $tmpText, "text output written");
    ok(-e $tmpJson, "JSON output written");
    _compare_json($discordance_sequenom, $tmpJson);
}

sub _read_without_filedate {
    # read a VCF file, omitting the fileDate line
    my $inPath = shift;
    my @buffer = ();
    open my $in, "<", $inPath ||
        log->logcroak("Cannot open input $inPath");
    while (<$in>) {
        if ( /^##fileDate/ ) { next; } # omit creation date for testing
        else { push(@buffer, $_); }
    }
    close $in || log->logcroak("Cannot close input $inPath");
    my $input = join("", @buffer);
    return $input;
}

sub _read_json {
    # read given path into a string and decode as JSON
    my $input = shift;
    open my $in, '<:encoding(utf8)', $input ||
        log->logcroak("Cannot open input '$input'");
    my $data = decode_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input '$input'");
    return $data;
}

sub _compare_json {
    # compare the contents of two JSON files
    # use to test genotype consistency check results against a master file
    my ($jsonPath0, $jsonPath1) = @_;
    open my $in, "<", $jsonPath0 ||
        log->logcroak("Cannot open input $jsonPath0");
    my $data0 = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $jsonPath0");
    open $in, "<", $jsonPath1 ||
        log->logcroak("Cannot open input $jsonPath1");
    my $data1 = from_json(join("", <$in>));
    close $in || log->logcroak("Cannot close input $jsonPath1");
    is_deeply($data0, $data1, "JSON data structures match");
}

sub _test_fluidigm_gtcheck {
    my $vcf = shift;
    my $gtcheck = WTSI::NPG::Genotyping::VCF::GtcheckWrapper->new();
    my ($resultRef, $maxDiscord) = $gtcheck->run($vcf);
    ok($resultRef, "Gtcheck result");
    is(scalar(keys(%{$resultRef})), 4, "Correct number of samples in result");
    my $outputJson = $tmp."/pairwise_discordance.json";
    my $outputText = $tmp."/pairwise_discordance.txt";
    ok($gtcheck->write_results_json($resultRef, $maxDiscord, $outputJson),
       "JSON output written");
    ok($gtcheck->write_results_text($resultRef, $maxDiscord, $outputText),
       "Text output written");
    _compare_json($discordance_fluidigm, $outputJson);
}

sub _test_sequenom_gtcheck {
    my $vcf = shift;
    my $gtcheck = WTSI::NPG::Genotyping::VCF::GtcheckWrapper->new();
    my ($resultRef, $maxDiscord) = $gtcheck->run($vcf);
    ok($resultRef, "Gtcheck result");
    is(scalar(keys(%{$resultRef})), 4, "Correct number of samples in result");
    my $outputJson = $tmp."/pairwise_discordance.json";
    my $outputText = $tmp."/pairwise_discordance.txt";
    ok($gtcheck->write_results_json($resultRef, $maxDiscord, $outputJson),
       "JSON output written");
    ok($gtcheck->write_results_text($resultRef, $maxDiscord, $outputText),
       "Text output written");
    _compare_json($discordance_sequenom, $outputJson)
}

sub _test_vcf_output {
    my ($outPath, $refPath) = @_;
    # read VCF output (omitting date) and compare to reference file
    my $outVCF = _read_without_filedate($outPath);
    my $refVCF = _read_without_filedate($refPath);
    is($outVCF, $refVCF, "Reference and output VCF files match");
}

sub _upload_fluidigm {
    my @csv_files = @fluidigm_csv;
    my $manifest = "qc_fluidigm_snp_info_GRCh37.tsv";
    return _upload_plex_files(\@csv_files, $manifest, 'qc',
                              $FLUIDIGM_TYPE);
}

sub _upload_sequenom {
    my @csv_files = @sequenom_csv;
    my $manifest = "W30467_snp_set_info_GRCh37.tsv";
    return _upload_plex_files(\@csv_files, $manifest, 'W30467',
                              $SEQUENOM_TYPE);
}

sub _upload_plex_files {
    # upload plex data to the test irods, including manifest & chromosomes
    # adds metadata to the manifest, denoting location of chromosome file
    # return the irods paths of the uploaded files
    # return array does *not* include the manifest & chromosome file
    my $irods = WTSI::NPG::iRODS->new;
    my @csv_files = @{ shift() };
    my $manifest = shift;
    my $manifest_value = shift;
    my $data_type = shift;
    my $manifest_key = $data_type.'_plex';
    my @inputs;
    foreach my $csv (@csv_files) {
        my $ipath = "$irods_tmp_coll/$csv";
        $irods->add_object("$data_path/$csv", $ipath);
        push(@inputs, $ipath);
    }
    my $manifest_path = "$data_path/$manifest";
    $irods->add_object($manifest_path, $irods_tmp_coll);
    $irods->add_object($chromosome_json_path, $irods_tmp_coll);
    my $man_obj = WTSI::NPG::iRODS::DataObject->new
        ($irods, "$irods_tmp_coll/$manifest")->absolute;
    $man_obj->add_avu
        ('chromosome_json', "$irods_tmp_coll/$chromosome_json_name");
    return @inputs;
}

return 1;
