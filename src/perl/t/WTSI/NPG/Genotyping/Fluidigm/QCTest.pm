use utf8;

package WTSI::NPG::Genotyping::Fluidigm::QCTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use File::Copy qw/copy/;
use File::Slurp qw/read_file/;
use File::Temp qw/tempdir/;
use Set::Scalar;
use Test::More tests => 17;
use Test::Exception;
use Text::CSV;

our $logconf = './etc/log4perl_tests.conf';
Log::Log4perl::init($logconf);
our $log = Log::Log4perl->get_logger();

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::QC'); }

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::QC;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata; # has attribute name constants
use WTSI::NPG::Utilities qw(md5sum);

my $script = 'qc_fluidigm.pl';
my $plate = '1381735059';
my $data_path = "./t/fluidigm_qc/$plate";
my $irods_tmp_coll;
my @irods_paths;
my $pid = $$;
my $tmp;
my $csv_name = 'fluidigm_qc.csv';
my $csv_name_outdated = 'fluidigm_qc_outdated_md5.csv';

sub make_fixture : Test(setup) {
    $tmp = tempdir('Fluidigm_QC_test_XXXXXX', CLEANUP => 1 );
    copy("./t/fluidigm_qc/$csv_name", $tmp);
    copy("./t/fluidigm_qc/$csv_name_outdated", $tmp);
    my $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = $irods->add_collection("FluidigmQCTest.$pid");
    $irods->put_collection($data_path, $irods_tmp_coll);
    foreach my $well (qw/S01 S02/) {
        my $name = $well.'_'.$plate.'.csv';
        my $irods_path = $irods_tmp_coll.'/'.$plate.'/'.$name;
        $irods->add_object_avu($irods_path, 'type', 'csv');
        $irods->add_object_avu($irods_path, $FLUIDIGM_PLATE_NAME, $plate);
        $irods->add_object_avu($irods_path, $FLUIDIGM_PLATE_WELL, $well);
        push @irods_paths, $irods_path;
    }
}

sub teardown : Test(teardown) {
    @irods_paths = ();
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
    require_ok('WTSI::NPG::Genotyping::Fluidigm::QC');
}

sub csv_output : Test(5) {
    my $qc = _create_qc_object();
    my $irods = WTSI::NPG::iRODS->new;
    my @data_objects;
    foreach my $obj_path (@irods_paths) {
        my $obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
            ($irods, $obj_path);
        push @data_objects, $obj;
    }
    my $fields;
    lives_ok(sub {$fields = $qc->csv_fields($data_objects[1]); },
             'CSV fields found OK');
    my $expected_fields = [
        'XYZ0987654321',
        '0.9231',
        96,
        94,
        70,
        70,
        96,
        26,
        24,
        '1381735059',
        'S02',
        '73ca301a0a9e1b9cf87d4daf59eb2815',
    ];
    is_deeply($fields, $expected_fields,
              'Field contents match expected values');
    my $string;
    lives_ok(sub {$string = $qc->csv_string($data_objects[1]); },
             'CSV string found OK');
    my $expected_string = 'XYZ0987654321,0.9231,96,94,70,70,96,26,24,'.
        '1381735059,S02,73ca301a0a9e1b9cf87d4daf59eb2815';
    ok($string eq $expected_string,
       'CSV string contents match expected values');

    $data_objects[0]->remove_avu($FLUIDIGM_PLATE_WELL, 'S01');
    dies_ok(sub { $qc->csv_fields($data_objects[0]); },
            'Dies without required metadata');
}

sub rewrite_existing : Test(2) {
    my $qc = _create_qc_object();
    my $output_path = "$tmp/rewritten.csv";
    open my $fh, ">", $output_path ||
        $log->logcroak("Cannot open CSV output ", $output_path);
    my $got_paths = $qc->rewrite_existing_csv($fh);
    close $fh || $log->logcroak("Cannot close CSV output ", $output_path);
    my $expected_paths = Set::Scalar->new();
    my $path = $irods_tmp_coll.'/1381735059/S01_1381735059.csv';
    $expected_paths->insert($path);
    is_deeply($got_paths, $expected_paths, 'Data object paths match');
    my $contents = read_file($output_path);
    my $md5 = '11413e77cde2a8dcca89705fe5b25a2d';
    my $expected = 'ABC0123456789,1.0000,96,96,70,70,96,26,26,1381735059'.
        ",S01,$md5\n";
    cmp_ok($contents, 'eq', $expected, 'Rewritten CSV file contents OK');
}

sub script_metaquery : Test(2) {
    my $cmd = "$script --query-path $irods_tmp_coll ".
        "--old-csv $tmp/$csv_name --in-place --logconf $logconf --debug";
    $log->info("Running command '$cmd'");
    ok(system($cmd)==0, "Script with --in-place and metaquery exits OK");
    my $msg = 'Script in-place CSV output matches expected values';
    _validate_csv_output("$tmp/$csv_name", $msg);
}

sub script_update : Test(2) {
    # ensure an entry with outdated md5 checksum is replaced
    my $cmd = "$script --query-path $irods_tmp_coll ".
        "--old-csv $tmp/$csv_name_outdated ".
            "--in-place --logconf $logconf --debug";
    $log->info("Running command '$cmd'");
    ok(system($cmd)==0, "Script with outdated input exits OK");
    _validate_csv_output("$tmp/$csv_name_outdated",
                         'Script updated md5 checksum in CSV');
}

sub script_stdin : Test(2) {
    my $fh;
    my $input_path = $tmp."/test_inputs.txt";
    open $fh, ">", $input_path ||
        $log->logcroak("Cannot open '$input_path'");
    foreach my $path (@irods_paths) {
        print $fh $path."\n";
    }
    close $fh || $log->logcroak("Cannot close '$input_path'");
    my $new_csv = "$tmp/fluidigm_qc_output.csv";
    my $cmd = "$script --new-csv $new_csv --old-csv $tmp/$csv_name ".
        "--logconf $logconf - < $input_path";
    $log->info("Running command '$cmd'");
    ok(system($cmd)==0, "Script with STDIN and new CSV file exits OK");
     _validate_csv_output($new_csv, 'Script CSV output OK, input from STDIN');
}

sub write_all : Test(2) {
    my $qc = _create_qc_object();
    my $output_path = "$tmp/qc_output.csv";
    open my $fh, ">", $output_path ||
        $log->logcroak("Cannot open CSV output ", $output_path);
    ok($qc->write_csv($fh), 'write_csv method returns OK');
    close $fh || $log->logcroak("Cannot close CSV output ", $output_path);
    _validate_csv_output($output_path, 'CSV output from QC object OK');
}

sub _create_qc_object {
    my $irods = WTSI::NPG::iRODS->new;
    # 1 of the 2 AssayDataObjects is already present in fluidigm_qc.csv
    # updated contents will contain QC results for the other AssayDataObject
    my $csv_path = "$tmp/$csv_name";
    my $qc = WTSI::NPG::Genotyping::Fluidigm::QC->new(
        csv_path     => $csv_path,
        data_object_paths => \@irods_paths,
    );
    return $qc;
}

sub _validate_csv_output {
    # check that CSV output matches the expected values
    # run an is_deeply test with the given message
    my ($csv_path, $message, ) = @_;
    # check the CSV output
    my $csv = Text::CSV->new ( { binary => 1 } );
    open my $fh, "<", "$csv_path" ||
        $log->logcroak("Cannot open input '$csv_path'");
    my $contents = $csv->getline_all($fh);
    close $fh || $log->logcroak("Cannot close input '$csv_path'");
    my $expected_contents = [
        [
            'ABC0123456789',
            '1.0000',
            96,
            96,
            70,
            70,
            96,
            26,
            26,
            '1381735059',
            'S01',
            '11413e77cde2a8dcca89705fe5b25a2d',
        ], [
            'XYZ0987654321',
            '0.9231',
            96,
            94,
            70,
            70,
            96,
            26,
            24,
            '1381735059',
            'S02',
            '73ca301a0a9e1b9cf87d4daf59eb2815',
        ],
    ];
    is_deeply($contents, $expected_contents, $message);
}


1;
