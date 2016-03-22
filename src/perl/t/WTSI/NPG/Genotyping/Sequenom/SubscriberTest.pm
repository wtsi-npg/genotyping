use utf8;

package WTSI::NPG::Genotyping::Sequenom::SubscriberTest;

use strict;
use warnings;
use DateTime;
use File::Path qw/make_path/;
use File::Slurp qw/read_file/;
use File::Spec::Functions qw/catfile/;
use File::Temp qw/tempdir/;
use JSON;
use List::AllUtils qw/uniq/;

use base qw(WTSI::NPG::Test);
use Test::More tests => 16;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

our $log = Log::Log4perl->get_logger();

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::Subscriber') };

use WTSI::NPG::Genotyping::Sequenom::Subscriber;
use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

use Data::Dumper; # FIXME development

my $data_path = './t/sequenom_subscriber';
my @assay_resultset_files = qw(sequenom_001.csv sequenom_002.csv
                               sequenom_003.csv sequenom_004.csv);
my @sample_identifiers = qw(sample_foo sample_foo sample_bar sample_baz);
my @sample_plates = qw(plate1234 plate1234 plate5678 plate1234);
my @sample_wells = qw(S01 S02 S03 S04);
my $non_unique_identifier = 'ABCDEFGHI';

my $reference_name = 'Homo_sapiens (GRCh37)';
my $snpset_name = 'W30467_GRCh37';
my $snpset_file = 'W30467_snp_set_info_GRCh37.tsv';
my $chromosome_length_file = 'chromosome_lengths_GRCh37.json';
my $tmp;

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = "SequenomSubscriberTest.$pid";
  $irods->add_collection($irods_tmp_coll);
  my $chromosome_lengths_irods = "$irods_tmp_coll/$chromosome_length_file";
  $irods->add_object("$data_path/$chromosome_length_file",
                     $chromosome_lengths_irods);
  $irods->add_object("$data_path/$snpset_file",
                     "$irods_tmp_coll/$snpset_file");

  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file")->absolute;
  $snpset_obj->add_avu('sequenom_plex', $snpset_name);
  $snpset_obj->add_avu('reference_name', $reference_name);
  $snpset_obj->add_avu('chromosome_json', $chromosome_lengths_irods);

  foreach my $i (0..3) {
    my $file = $assay_resultset_files[$i];
    $irods->add_object("$data_path/$file", "$irods_tmp_coll/$file");
    my $resultset_obj = WTSI::NPG::iRODS::DataObject->new
      ($irods,"$irods_tmp_coll/$file")->absolute;
    $resultset_obj->add_avu('sequenom_plex', $snpset_name);
    $resultset_obj->add_avu('sequenom_plate', $sample_plates[$i]);
    $resultset_obj->add_avu('sequenom_well', $sample_wells[$i]);
    $resultset_obj->add_avu('dcterms:identifier', $sample_identifiers[$i]);
    $resultset_obj->add_avu('dcterms:identifier', $non_unique_identifier);
  }

 # set up dummy fasta reference
  $tmp = tempdir("sequenom_subscriber_test_XXXXXX", CLEANUP => 1);
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


sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::Subscriber');
}

sub constructor : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  new_ok('WTSI::NPG::Genotyping::Sequenom::Subscriber',
         [irods          => $irods,
          data_path      => $irods_tmp_coll,
          reference_path => $irods_tmp_coll,
          reference_name => $reference_name,
          snpset_name    => $snpset_name]);
}

sub find_object_paths : Test(1) {
    my $irods = WTSI::NPG::iRODS->new;
    my @obj_paths = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_tmp_coll,
     reference_path => $irods_tmp_coll,
     reference_name => $reference_name,
     snpset_name    => $snpset_name)->find_object_paths(
         [uniq @sample_identifiers]);
    ok(scalar @obj_paths == 4,
       "Found 4 iRODS object paths for sample results");
}

sub get_assay_resultsets : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $resultsets1 = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_tmp_coll,
     reference_path => $irods_tmp_coll,
     reference_name => $reference_name,
     snpset_name    => $snpset_name)->get_assay_resultsets
       ([uniq @sample_identifiers]);

  cmp_ok(scalar keys %$resultsets1, '==', 3, 'Assay resultsets for 3 samples');
  cmp_ok(scalar @{$resultsets1->{sample_foo}}, '==', 2,
         '2 of 4 results for 1 sample');
  cmp_ok(scalar @{$resultsets1->{sample_bar}}, '==', 1,
         '1 of 4 results for 1 sample');

  dies_ok {
    WTSI::NPG::Genotyping::Sequenom::Subscriber->new
        (irods          => $irods,
         data_path      => $irods_tmp_coll,
         reference_path => $irods_tmp_coll,
         reference_name => $reference_name,
         snpset_name    => $snpset_name)->get_assay_resultsets
           ([$non_unique_identifier]);
  } 'Fails when query finds results for >1 sample';

  ok(defined WTSI::NPG::Genotyping::Sequenom::Subscriber->new
     (irods          => $irods,
      data_path      => $irods_tmp_coll,
      reference_path => $irods_tmp_coll,
      reference_name => $reference_name,
      snpset_name    => $snpset_name)->get_assay_resultsets
     ([map { 'X' . $_ } 1 .. 100]), "'IN' query of 100 args");
}

sub get_assay_resultsets_and_vcf_metadata : Test(3) {

  my $irods = WTSI::NPG::iRODS->new;

  my $subscriber = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
      (irods          => $irods,
       data_path      => $irods_tmp_coll,
       reference_path => $irods_tmp_coll,
       reference_name => $reference_name,
       snpset_name    => $snpset_name);

  my ($resultsets_index, $vcf_meta) =
      $subscriber->get_assay_resultsets_and_vcf_metadata(
          [uniq @sample_identifiers]);

  ok($vcf_meta, "VCF metadata found");
  my $expected_meta = {
      'plex_type' => [ 'sequenom' ],
      'plex_name' => [ 'W30467_GRCh37' ],
      'callset_name' => [ 'sequenom_W30467_GRCh37' ]
  };
  is_deeply($vcf_meta, $expected_meta,
            "VCF metadata matches expected values");
  ok(scalar keys %{$resultsets_index} == 3,
     "Found resultset index for 3 sample identifiers");
}

sub get_chromosome_lengths : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $chr_lengths = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_tmp_coll,
     reference_path => $irods_tmp_coll,
     reference_name => $reference_name,
     snpset_name    => $snpset_name)->get_chromosome_lengths();
  ok($chr_lengths, 'Chromosome lengths found');
  my $chromosome_length_path = "$data_path/$chromosome_length_file";
  my $chr_lengths_expected = decode_json(read_file($chromosome_length_path));
  is_deeply($chr_lengths, $chr_lengths_expected,
            "Chromosome lengths match expected values");
}

sub get_vcf_metadata : Test(2) {
    my $irods = WTSI::NPG::iRODS->new;

    my $subscriber =  WTSI::NPG::Genotyping::Sequenom::Subscriber->new
    (irods          => $irods,
     data_path      => $irods_tmp_coll,
     reference_path => $irods_tmp_coll,
     reference_name => $reference_name,
     snpset_name    => $snpset_name);

    my @obj_paths = $subscriber->find_object_paths(
        [uniq @sample_identifiers]);
    my @data_objects = map {
        WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
              ($irods, $_);
    } @obj_paths;
    my $vcf_meta = $subscriber->vcf_metadata_from_irods(\@data_objects);
    ok($vcf_meta, "VCF metadata found");
    my $expected_meta = {
          'plex_type' => [ 'sequenom' ],
          'plex_name' => [ 'W30467_GRCh37' ],
          'callset_name' => [ 'sequenom_W30467_GRCh37' ]
      };
    is_deeply($vcf_meta, $expected_meta,
              "VCF metadata matches expected values");
}

