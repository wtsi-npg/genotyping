package WTSI::NPG::Genotyping::VCF::VCFTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Cwd qw(abs_path);
use File::Path qw/make_path/;
use File::Slurp qw /read_file/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw(tempdir);
use JSON;
use Test::More;
use Test::Exception;

use WTSI::NPG::iRODS;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $log = Log::Log4perl->get_logger();

our $tmp;
our @fluidigm_csv = qw/fluidigm_001.csv fluidigm_002.csv
                       fluidigm_003.csv fluidigm_004.csv/;
our @sequenom_csv = qw/sequenom_001.csv sequenom_002.csv
                       sequenom_003.csv sequenom_004.csv/;

our $chromosome_lengths;

our $SEQUENOM_TYPE = 'sequenom'; # TODO avoid repeating these across modules
our $FLUIDIGM_TYPE = 'fluidigm';

our $REFERENCE_NAME = 'Homo_sapiens (GRCh37_53)';

use WTSI::NPG::Genotyping::Call;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::VCF::AssayResultParser;
use WTSI::NPG::Genotyping::VCF::DataRowParser;
use WTSI::NPG::Genotyping::VCF::HeaderParser;
use WTSI::NPG::Genotyping::VCF::GtcheckWrapper;
use WTSI::NPG::Genotyping::VCF::Slurper;

my $data_path = './t/vcf';
my $sequenom_snpset_name = 'W30467_snp_set_info_GRCh37.tsv';
my $sequenom_snpset_path = $data_path.'/'.$sequenom_snpset_name;
my $fluidigm_snpset_name = "qc_fluidigm_snp_info_GRCh37.tsv";
my $fluidigm_snpset_path = $data_path."/".$fluidigm_snpset_name;
my $chromosome_json_name = 'chromosome_lengths_GRCh37.json';
my $chromosome_json_path = $data_path.'/'.$chromosome_json_name;
my $discordance_fluidigm = $data_path."/pairwise_discordance_fluidigm.json";
my $discordance_sequenom = $data_path."/pairwise_discordance_sequenom.json";
my $vcf_fluidigm = $data_path."/fluidigm.vcf";
my $vcf_fluidigm_header_1 = $data_path."/fluidigm_header_1.txt";
my $vcf_fluidigm_header_2 = $data_path."/fluidigm_header_2.txt";
my $vcf_sequenom = $data_path."/sequenom.vcf";

my $irods;
my $irods_tmp_coll;
my $pid = $$;
my $testnum = 0; # use to create distinct temporary irods collections

my $reference_vcf_meta;

sub setup: Test(setup) {
    $tmp = tempdir("vcftest_XXXXXX", CLEANUP => 1);
    $reference_vcf_meta = 'file://'.abs_path($tmp).'/references/'.
        'Homo_sapiens/GRCh37_53/all/'.
        'fasta/Homo_sapiens.GRCh37.dna.all.fa';
    $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = "VCFTest.$pid.$testnum";
    $testnum++;
    $irods->add_collection($irods_tmp_coll);
    $irods_tmp_coll = $irods->absolute_path($irods_tmp_coll);
    # read chromosome lengths
    $chromosome_lengths = _read_json($chromosome_json_path);
    # set up dummy fasta reference
    $ENV{NPG_REPOSITORY_ROOT} = $tmp;
    my $fastadir = catfile($tmp, 'references', 'Homo_sapiens',
                           'GRCh37_53', 'all', 'fasta');
    make_path($fastadir);
    my $reference_file_path = catfile($fastadir,
                                      'Homo_sapiens.GRCh37.dna.all.fa');
    open my $fh, '>>', $reference_file_path || $log->logcroak(
        "Cannot open reference file path '", $reference_file_path, "'");
    close $fh || $log->logcroak(
        "Cannot close reference file path '", $reference_file_path, "'");
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}

sub data_row_parser_test : Test(5) {
    my $fh;
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($fluidigm_snpset_path);
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    new_ok('WTSI::NPG::Genotyping::VCF::DataRowParser',
           [input_filehandle => $fh, snpset => $snpset] );
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    my $parser = WTSI::NPG::Genotyping::VCF::DataRowParser->new(
        input_filehandle => $fh, snpset => $snpset );
    isa_ok($parser, 'WTSI::NPG::Genotyping::VCF::DataRowParser');
    my $first_row = $parser->get_next_data_row();
    isa_ok($first_row, 'WTSI::NPG::Genotyping::VCF::DataRow');
    my @fields = qw(1	74941293	rs649058	G	A	.
                    .	ORIGINAL_STRAND=+	GT:GQ:DP	0/1:40:1
                    0/1:40:1	0/1:40:1	0/1:40:1);
    my $expected_first_row = join "\t", @fields;
    is($first_row->str, $expected_first_row,
       'First row string matches expected value');
    my $other_rows = $parser->get_all_remaining_rows();
    is(scalar @{$other_rows}, 25, 'Found expected number of other rows');
    close $fh || die "Cannot close VCF $vcf_fluidigm";

}

sub fluidigm_file_test : Test(204) {
    my @inputs;
    foreach my $name (@fluidigm_csv) {
        push(@inputs, abs_path($data_path."/".$name));
    }
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($fluidigm_snpset_path);
    my %metadata = (
        reference    => [ $reference_vcf_meta ],
        plex_type    => [ $FLUIDIGM_TYPE ],
        plex_name    => [ 'qc' ],
        callset_name => [ 'fluidigm_qc' ],
    );
    my @resultsets;
    foreach my $input (@inputs) {
        my $result = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
            $input);
        push @resultsets, $result;
    }
    my $parser = WTSI::NPG::Genotyping::VCF::AssayResultParser->new
        (resultsets => \@resultsets,
         assay_snpset => $snpset,
         contig_lengths => $chromosome_lengths,
         metadata => \%metadata);
    my $vcf_dataset = $parser->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_fluidigm.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Fluidigm results to VCF with input from file");
    _test_vcf_output($vcf_fluidigm, $vcf_file, 'fluidigm_file_test');
    _test_fluidigm_gtcheck($vcf_file);
    # test the calls_by_sample method of VCFDataSet
    my $calls_by_sample = $vcf_dataset->calls_by_sample();
    is(scalar keys %{$calls_by_sample}, 4, "Correct number of samples");
    foreach my $sample ( keys %{$calls_by_sample} ) {
        my @calls = @{$calls_by_sample->{$sample}};
        is(scalar @calls, 24, "Correct number of calls for $sample");
        foreach my $call (@calls) {
            isa_ok($call, 'WTSI::NPG::Genotyping::Call');
            ok($call->callset_name eq 'fluidigm_qc', "Callset name OK");
        }
    }
}

sub fluidigm_irods_test : Test(7) {
    # upload test data to temporary irods collection
    my @inputs = _upload_fluidigm();
    my @resultsets;
    foreach my $input (@inputs) {
        my $data_obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new(
            $irods, $input);
        my $result = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
            $data_obj);
        push @resultsets, $result;
    }
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($fluidigm_snpset_path);
    my %metadata = (
        reference    => [ $reference_vcf_meta ],
        plex_type    => [ $FLUIDIGM_TYPE ],
        plex_name    => [ 'qc' ],
        callset_name => [ 'fluidigm_qc' ]
    ); # hash of arrayrefs for compatibility with VCF header
    my $parser = WTSI::NPG::Genotyping::VCF::AssayResultParser->new
        (resultsets => \@resultsets,
         assay_snpset => $snpset,
         contig_lengths => $chromosome_lengths,
         metadata => \%metadata,
         );
    my $vcf_dataset = $parser->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_fluidigm.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Fluidigm results to VCF with input from iRODS");
    _test_vcf_output($vcf_fluidigm, $vcf_file, 'fluidigm_irods_test');
    _test_fluidigm_gtcheck($vcf_file);
}

sub header_test : Test(4) {
    # test the contig_to_string and parse_contig_line methods of Header.pm
    my $samples = ['foo', 'bar'];
    my @contig_strings = (
        '<ID=1,length=249250621,species="Homo sapiens">',
        '<ID=2,length=243199373,species="Homo sapiens">',
    );
    my %metadata = (reference => [ $reference_vcf_meta, ],
                    source    => [ 'WTSI_NPG_genotyping_pipeline', ],
                    contig    => \@contig_strings,
                );
    new_ok('WTSI::NPG::Genotyping::VCF::Header',
           [sample_names => $samples,
            metadata     => \%metadata]);
    my $header = WTSI::NPG::Genotyping::VCF::Header->new(
        sample_names => $samples,
        metadata     => \%metadata,
        );
    my $contig = 2;
    my $length = 243199373;
    my $str = $header->contig_to_string($contig, $length);
    is($str, $contig_strings[1],
       'Contig string output equals expected value');
    my $line = '##contig='.$str;
    my ($parsed_contig, $parsed_length) = $header->parse_contig_line($line);
    is($parsed_contig, $contig, "Parsed contig name matches original");
    is($parsed_length, $length, "Parsed contig length matches original");
}

sub header_parser_test : Test(6) {
    my $fh;
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    new_ok('WTSI::NPG::Genotyping::VCF::HeaderParser',
           [input_filehandle => $fh] );
    my $parser1 = WTSI::NPG::Genotyping::VCF::HeaderParser->new(
        input_filehandle => $fh);
    my $header1 = $parser1->header;
    isa_ok($header1, 'WTSI::NPG::Genotyping::VCF::Header');
    my $header1_expected = _read_without_filedate($vcf_fluidigm_header_1);
    is_deeply(_remove_filedate_reference($header1->str), $header1_expected,
       'Parsed header string matches expected value');
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    my @samples = qw(north south east west);
    my $parser2 = WTSI::NPG::Genotyping::VCF::HeaderParser->new(
        input_filehandle => $fh, sample_names => \@samples);
    my $header2 = $parser2->header;
    isa_ok($header2, 'WTSI::NPG::Genotyping::VCF::Header');
    my $header2_expected = _read_without_filedate($vcf_fluidigm_header_2);
    is_deeply(_remove_filedate_reference($header2->str), $header2_expected,
        'Parsed header string matches expected value, alternate samples');
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    my @wrong_samples = qw(north south east west up down);
    dies_ok {
        my $hp = WTSI::NPG::Genotyping::VCF::HeaderParser->new(
            input_filehandle => $fh, 'sample_names' => \@wrong_samples,
        );
        my $header = $hp->header;
     } 'Dies with incorrect number of sample names';
    close $fh || die "Cannot close VCF $vcf_fluidigm";
}

sub sequenom_file_test : Test(7) {
    # sequenom test with input from local filesystem
    my @inputs;
    foreach my $name (@sequenom_csv) {
        push @inputs, abs_path($data_path."/".$name);
    }
    my @resultsets;
    foreach my $input (@inputs) {
        my $result = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
            $input);
        push @resultsets, $result;
    }
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($sequenom_snpset_path);
    my %metadata = (
        reference => [ $reference_vcf_meta ],
        plex_type => [ $SEQUENOM_TYPE ],
        plex_name => [ 'W30467' ],
        callset_name => [ 'sequenom_W30467' ]
    );
    my $parser = WTSI::NPG::Genotyping::VCF::AssayResultParser->new
        (resultsets => \@resultsets,
         assay_snpset => $snpset,
         contig_lengths => $chromosome_lengths,
         metadata => \%metadata);
    my $vcf_dataset = $parser->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_sequenom.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Sequenom results to VCF with input from file");
    _test_vcf_output($vcf_sequenom, $vcf_file, 'sequenom_file_test');
    _test_sequenom_gtcheck($vcf_file);
}

sub sequenom_irods_test : Test(7) {
    # upload test data to temporary irods collection
    my @inputs = _upload_sequenom();
    my @resultsets;
    foreach my $input (@inputs) {
        my $data_obj = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new(
            $irods, $input);
        my $result = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
            $data_obj);
        push @resultsets, $result;
    }
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($sequenom_snpset_path);
    my %metadata = (
        reference    => [ $reference_vcf_meta ],
        plex_type    => [ $SEQUENOM_TYPE ],
        plex_name    => [ 'W30467' ],
        callset_name => [ 'sequenom_W30467' ]
    );
    my $parser = WTSI::NPG::Genotyping::VCF::AssayResultParser->new
        (resultsets => \@resultsets,
         assay_snpset => $snpset,
         contig_lengths => $chromosome_lengths,
         metadata => \%metadata);
    my $vcf_dataset = $parser->get_vcf_dataset();
    my $vcf_file = $tmp.'/conversion_test_sequenom.vcf';
    ok($vcf_dataset->write_vcf($vcf_file),
       "Converted Sequenom results to VCF with input from iRODS");
    _test_vcf_output($vcf_sequenom, $vcf_file, 'sequenom_irods_test');
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
        "--snpset $snpset_ipath --irods --plex_type ".$SEQUENOM_TYPE." ".
        "--callset ".$SEQUENOM_TYPE."_W30467 ".
        "--repository $tmp < $sequenomList";
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $vcfOutput, "VCF output written");
    # read VCF output (omitting date) and compare to reference file
    my @buffer = ();
    _test_vcf_output($vcf_sequenom, $vcfOutput, 'script_conversion_test');
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
            "--quiet --chromosomes $chromosome_json_path ".
	    "--plex_type sequenom --repository $tmp ",
	"$checker --input - --text $tmpText --json $tmpJson"
    );
    my $cmd = join(' | ', @cmds);
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $tmpText, "text output written");
    ok(-e $tmpJson, "JSON output written");
    _compare_json($discordance_sequenom, $tmpJson);
}

sub script_plink_test : Test(3) {
    my $script = "bin/vcf_from_plink.pl";
    my $contig = "$data_path/chromosome_lengths_GRCh37.json";
    my $manifest = "$data_path/W30467_snp_set_info_GRCh37.tsv";
    my $plink = "$data_path/fake_qc_genotypes";
    my $vcf = "$tmp/plink_converted.vcf";
    my $cmd = "$script --contigs $contig --manifest $manifest ".
        "--plink $plink --vcf $vcf";
    is(system($cmd), 0, "$cmd exits successfully");
    ok(-e $vcf, "VCF output written");
    my $expected_vcf = "$data_path/calls_from_plink.vcf";
    my $expected_lines = _read_without_filedate($expected_vcf);
    my $got_lines = _read_without_filedate($vcf);
    is_deeply($got_lines, $expected_lines,
              "VCF output matches expected values");
}


sub slurp_test : Test(7) {
    my $vcfName = "fluidigm.vcf";
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($fluidigm_snpset_path);
    my ($fh, $slurper, $dataset, $got_vcf);
    my $expected_vcf = _read_without_filedate($vcf_fluidigm);
    # object creation with snpset
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    new_ok('WTSI::NPG::Genotyping::VCF::Slurper',
           [ input_filehandle=> $fh, snpset => $snpset ] );
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    # object creation with snpset path hash
    _upload_fluidigm(); # uploads manifest to irods tmp collection
    my %snpset_paths;
    my $snpset_ipath = $irods_tmp_coll."/".$fluidigm_snpset_name;
    $snpset_paths{'fluidigm'}{'qc'} = $snpset_ipath;
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    new_ok('WTSI::NPG::Genotyping::VCF::Slurper',
           [ input_filehandle=> $fh, snpset_irods_paths => \%snpset_paths ]);
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    # reading dataset with snpset
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    $slurper = WTSI::NPG::Genotyping::VCF::Slurper->new(
        input_filehandle=> $fh, snpset => $snpset
    );
    $dataset = $slurper->read_dataset();
    isa_ok($dataset, 'WTSI::NPG::Genotyping::VCF::VCFDataSet');
    $got_vcf = _remove_filedate_reference($dataset->str());
    is_deeply($got_vcf, $expected_vcf, 'Parsed output matches input');
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    # reading dataset with snpset path hash
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    $slurper = WTSI::NPG::Genotyping::VCF::Slurper->new(
        input_filehandle=> $fh, snpset_irods_paths => \%snpset_paths,
    );
    $dataset = $slurper->read_dataset();
    isa_ok($dataset, 'WTSI::NPG::Genotyping::VCF::VCFDataSet');
    $got_vcf = _remove_filedate_reference($dataset->str());
    is_deeply($got_vcf, $expected_vcf, 'Parsed output matches input');
    close $fh || die "Cannot close VCF $vcf_fluidigm";
    # test with different sample names
    my @sample_names = qw(north south east west);
    open $fh, '<', $vcf_fluidigm || die "Cannot open VCF $vcf_fluidigm";
    my $slurper_alt_names = WTSI::NPG::Genotyping::VCF::Slurper->new(
        input_filehandle=> $fh,
        snpset => $snpset,
        sample_names => \@sample_names,
    );
    my $dataset_alt_names = $slurper_alt_names->read_dataset();
    isa_ok($dataset_alt_names, 'WTSI::NPG::Genotyping::VCF::VCFDataSet');
    close $fh || die "Cannot close VCF $vcf_fluidigm";
}


sub vcf_dataset_test: Test(4) {
    my $samples = ['foo', 'bar'];
    my @contig_strings = (
        '<ID=1,length=249250621,species="Homo sapiens">',
        '<ID=2,length=243199373,species="Homo sapiens">',
    );
    my %metadata = (reference => [ $reference_vcf_meta, ],
                    source    => [ 'WTSI_NPG_genotyping_pipeline', ],
                    contig    => \@contig_strings,
                );
    my $header = WTSI::NPG::Genotyping::VCF::Header->new(
        sample_names => $samples,
        metadata     => \%metadata,
        );
    my $manifest = "$data_path/W30467_snp_set_info_GRCh37.tsv";
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($manifest);
    my $snp = $snpset->named_snp('rs649058');
    my $call_1 = WTSI::NPG::Genotyping::Call->new(
        snp => $snp, genotype => 'AA'
    );
    my $call_2 = WTSI::NPG::Genotyping::Call->new(
        snp => $snp, genotype => 'GA'
    );
    my $gm = $snpset->named_snp('GS34251'); # gendermarker
    my $call_3 = WTSI::NPG::Genotyping::Call->new(
        snp => $gm->x_marker, genotype => 'TT'
    );
    my $call_4 = WTSI::NPG::Genotyping::Call->new(
        snp => $gm->x_marker, genotype => 'TT'
    );
    my $call_5 = WTSI::NPG::Genotyping::Call->new(
        snp => $gm->y_marker, genotype => 'CC'
    );
    my $call_6 = WTSI::NPG::Genotyping::Call->new(
        snp => $gm->y_marker, genotype => 'NN', is_call => 0,
    );
    new_ok('WTSI::NPG::Genotyping::VCF::DataRow',
           [calls => [$call_1, $call_2]]);
    my $data_rows = [
        WTSI::NPG::Genotyping::VCF::DataRow->new(calls => [$call_1, $call_2]),
        WTSI::NPG::Genotyping::VCF::DataRow->new(calls => [$call_3, $call_4]),
        WTSI::NPG::Genotyping::VCF::DataRow->new(calls => [$call_5, $call_6]),
    ];
    new_ok('WTSI::NPG::Genotyping::VCF::VCFDataSet',
           [header => $header, data => $data_rows ]);
    my $dataset = WTSI::NPG::Genotyping::VCF::VCFDataSet->new(
        header => $header,
        data => $data_rows,
    );
    my %cbs = %{$dataset->calls_by_sample()};
    is(scalar keys %cbs, 2, "2 samples found in VCFDataSet");
    # X and Y calls have been merged into a GenderMarkerCall
    is(scalar @{$cbs{'foo'}}, 2, "2 calls found for sample foo");
}

sub _read_without_filedate {
    # read a VCF file, omitting the ##fileDate and ##reference lines
    # duplicated in ReadyWorkflowTest.pm
    my ($inPath) = @_;
    my $lines = read_file($inPath);
    return _remove_filedate_reference($lines);
}

sub _remove_filedate_reference {
    # remove the fileDate and reference from a string containing VCF
    # return an ArrayRef[Str] containing data
    my ($vcf_str) = @_;
    my @lines_in = split m/\n/msx, $vcf_str;
    my @lines_out;
    foreach my $line (@lines_in) {
        if ( $line =~ /^[#]{2}(fileDate|reference)/msx ) { next; }
        else { push(@lines_out, $line); }
    }
    return \@lines_out;
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
    my ($outPath, $refPath, $name) = @_;
    # read VCF output (omitting date) and compare to reference file
    my $gotVCF = _read_without_filedate($outPath);
    my $expectedVCF = _read_without_filedate($refPath);
    is_deeply($gotVCF, $expectedVCF, "VCF outputs match for test $name");
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
        my $obj =  WTSI::NPG::iRODS::DataObject->new($irods, $ipath);
        $obj->add_avu($data_type.'_plex', $manifest_value);
        $obj->add_avu('reference', $REFERENCE_NAME);
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
