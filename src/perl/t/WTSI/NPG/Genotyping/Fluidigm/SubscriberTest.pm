
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::SubscriberTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 7;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber') };

use WTSI::NPG::Genotyping::Fluidigm::Subscriber;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

my $data_path = './t/fluidigm_subscriber';
my @assay_resultset_files = qw(S01_1381735059.csv S02_1381735059.csv);
my @sample_identifiers = qw(ABC0123456789 XYZ0123456789);
my $non_unique_identifier = 'ABCDEFGHI';
my $snpset_file = 'qc.csv';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = "FluidigmSubscriberTest.$pid";
  $irods->add_collection($irods_tmp_coll);
  $irods->add_object("$data_path/$snpset_file", "$irods_tmp_coll/$snpset_file");

  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$snpset_file")->absolute;
  $snpset_obj->add_avu('fluidigm_plex', 'qc');
  $snpset_obj->add_avu('reference_name', 'Homo_sapiens (1000Genomes)');

  foreach my $i (0..1) {
    my $file = $assay_resultset_files[$i];
    $irods->add_object("$data_path/$file", "$irods_tmp_coll/$file");
    my $resultset_obj = WTSI::NPG::iRODS::DataObject->new
      ($irods,"$irods_tmp_coll/$file")->absolute;
    $resultset_obj->add_avu('fluidigm_plex', 'qc');
    $resultset_obj->add_avu('fluidigm_plate', '1381735059');
    $resultset_obj->add_avu('fluidigm_well', 'S0' . $i);
    $resultset_obj->add_avu('dcterms:identifier', $sample_identifiers[$i]);
    $resultset_obj->add_avu('dcterms:identifier', $non_unique_identifier);
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber');
}

sub constructor : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  new_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber',
         [irods => $irods]);
}

sub get_assay_resultsets : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my @resultsets = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods => $irods)->get_assay_resultsets('qc', $non_unique_identifier);

  cmp_ok(scalar @resultsets, '==', 2, 'Assay resultsets');
}

sub get_assay_resultset : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $resultset = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
    (irods => $irods)->get_assay_resultset('qc', 'ABC0123456789');

  ok($resultset, 'Assay resultsets');
  dies_ok {
    WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
        (irods => $irods)->get_assay_resultset('qc', $non_unique_identifier);
  } 'Fails on matching multiple results';
}

sub get_calls : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  my @calls = @{WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
      (irods => $irods)->get_calls('Homo_sapiens (1000Genomes)', 'qc',
                                   'ABC0123456789')};
  cmp_ok(scalar @calls, '==', 26, 'Number of calls');
}

1;
