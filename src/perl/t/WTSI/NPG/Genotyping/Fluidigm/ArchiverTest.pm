use utf8;


{
  package WTSI::NPG::Database::WarehouseStub;
  # needed for Fluidigm publisher

  use Moose;
  use Carp;

  extends 'WTSI::NPG::Database';

  has 'test_id_lims' =>
    (is       => 'rw',
     isa      => 'Str',
     required => 0,
     default  => sub { 'SQSCP' });

  has 'test_sanger_sample_id' =>
    (is       => 'rw',
     isa      => 'Str | Undef',
     required => 0,
     default  => sub { '0123456789' });

  sub find_fluidigm_sample_by_plate {
    my ($self, $fluidigm_barcode, $well) = @_;

    $well eq 'S01' or
      confess "WarehouseStub expected well argument 'S01' but got '$well'";

    return {id_lims            => $self->test_id_lims,
            id_sample_lims     => 123456789,
            sanger_sample_id   => $self->test_sanger_sample_id,
            consent_withdrawn  => 0,
            donor_id           => 'D999',
            uuid               => 'AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDD',
            name               => 'sample1',
            common_name        => 'Homo sapiens',
            supplier_name      => 'aaaaaaaaaa',
            accession_number   => 'A0123456789',
            gender             => 'Female',
            cohort             => 'AAA111222333',
            control            => 'XXXYYYZZZ',
            study_id           => 0,
            barcode_prefix     => 'DN',
            barcode            => '0987654321',
            plate_purpose_name => 'Fluidigm',
            map                => 'S01'};
  }
}


package WTSI::NPG::Genotyping::Fluidigm::ArchiverTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Archive::Tar;
use Cwd qw(abs_path);
use File::Temp qw(tempdir);
use Test::More tests => 8;
use Test::Exception;
use WTSI::NPG::Database::WarehouseStub;
use WTSI::NPG::iRODS;
use WTSI::NPG::Genotyping::Fluidigm::Archiver;
use WTSI::NPG::Genotyping::Fluidigm::Publisher;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;
use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::DataObject;

use Log::Log4perl;
Log::Log4perl::init('./etc/log4perl_tests.conf');
our $log = Log::Log4perl->get_logger();

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Archiver'); }

my $data_path = './t/fluidigm_archiver';
my $fluidigm_directory_name = "0123456789";
my $fluidigm_directory = $data_path."/".$fluidigm_directory_name;
my $snpset_file = 'qc.tsv';
my $tmp;
my $fluidigm_tmp;
my $resultset;
my $reference_path;
my $irods_tmp_coll;
my $pid = $$;

sub make_fixture : Test(setup) {
    # create a Fluidigm resultset and publish to temp iRODS collection
    my $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = "FluidigmPublisherTest.$pid";
    $irods->add_collection($irods_tmp_coll);
    $irods->add_object("$data_path/$snpset_file",
                       "$irods_tmp_coll/$snpset_file");
    $reference_path = WTSI::NPG::iRODS::Collection->new
        ($irods, "$irods_tmp_coll" )->absolute->str;
    my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
        ($irods, "$irods_tmp_coll/$snpset_file" )->absolute;
    $snpset_obj->add_avu('fluidigm_plex', 'qc');
    $snpset_obj->add_avu('reference_name', 'Homo_sapiens (1000Genomes)');
    my $publication_time = DateTime->now;
    my $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
        (directory => $fluidigm_directory);
    my $inifile = File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini');
    my $whdb = WTSI::NPG::Database::WarehouseStub->new
        (name         => 'ml_warehouse',
         inifile      => $inifile,
         test_id_lims => 'SQSCP');
    my $publisher = WTSI::NPG::Genotyping::Fluidigm::Publisher->new
        (publication_time => $publication_time,
         resultset        => $resultset,
         reference_path   => $reference_path,
         warehouse_db     => $whdb);
    $publisher->publish($irods_tmp_coll);
    $tmp = tempdir('FluidigmArchiverTest_XXXXXX', CLEANUP => 1);
    $fluidigm_tmp = File::Spec->catfile($tmp, $fluidigm_directory_name);
    `cp -R $fluidigm_directory $tmp`;
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
    undef $resultset;
}


sub irods_publication : Test(1) {
    my $irods = WTSI::NPG::iRODS->new;
    my $archiver = WTSI::NPG::Genotyping::Fluidigm::Archiver->new
        (irods      => $irods,
         irods_root => $irods_tmp_coll,
         output_dir => $tmp,
         days_ago    => 90,
        );
    ok($archiver->irods_publication_ok($fluidigm_tmp),
       "$fluidigm_tmp published to iRODS OK");
}

sub ready_to_archive : Test(3) {
    my $irods = WTSI::NPG::iRODS->new;
    my $archiver = WTSI::NPG::Genotyping::Fluidigm::Archiver->new
        (irods      => $irods,
         irods_root => $irods_tmp_coll,
         output_dir => $tmp,
         days_ago    => 90,
        );
    my $fluidigm_regex = qr{^\d{10}$}msxi;

    # set mtime of test data to the current date/time
    `find $fluidigm_tmp | xargs touch`;
    my @to_archive_new = $archiver->find_directories_to_archive
        ($tmp, $fluidigm_regex);
    is_deeply(\@to_archive_new, [ ],
              "New directory is not OK for archiving");

    # reset mtime to an old date/time
    `find $fluidigm_tmp | xargs touch -d 2000-01-01`;
    my @to_archive_old = $archiver->find_directories_to_archive
        ($tmp, $fluidigm_regex);
    is_deeply(\@to_archive_old, [ abs_path($fluidigm_tmp), ],
              "Old directory is OK for archiving");

    # remove iRODS publication metadata; causes check to fail
    my @results = $irods->find_objects_by_meta(
        $irods_tmp_coll,
        [fluidigm_plate => '1381735059'],
        [fluidigm_well => 'S70'],
    );
    my $result = shift @results;
    $irods->remove_object_avu($result, 'fluidigm_plate', '1381735059');
    my @to_archive_unpub = $archiver->find_directories_to_archive
        ($tmp, $fluidigm_regex);
    is_deeply(\@to_archive_unpub, [ ],
              "Unpublished directory is not OK for archiving");
}

sub script : Test(3) {
    # reset mtime to an old date/time
    `find $fluidigm_tmp | xargs touch -d 2000-01-01`;
    my $script = './bin/archive_fluidigm_genotypes.pl';
    ok(system(join q{ }, "$script",
              "--input_dir $tmp",
              "--irods_root $irods_tmp_coll",
              "--output_dir $tmp",
              "--debug",
              "2>/tmp/fluidigm.txt",
          ) == 0, "$script ran with zero exit status");
    my $archive_file =  $tmp."/fluidigm_2000-01.tar.gz";
    ok(-e $archive_file, 'Expected .tar.gz output exists');
    my $tar = Archive::Tar->new();
    $tar->read($archive_file);
    my @contents = $tar->list_files();
    is(scalar @contents, 6, 'Correct number of entries in .tar.gz archive');
}
