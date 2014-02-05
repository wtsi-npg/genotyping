
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::SubscriberTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 4;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::Subscriber') };

use WTSI::NPG::Genotyping::Fluidigm::Subscriber;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

my $data_path = './t/fluidigm_subscriber';
my $assay_resultset_file = 'S01_1381735059.csv';
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

  $irods->add_object("$data_path/$assay_resultset_file",
                     "$irods_tmp_coll/$assay_resultset_file");
  my $resultset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods,"$irods_tmp_coll/$assay_resultset_file")->absolute;
  $resultset_obj->add_avu('fluidigm_plex', 'qc');
  $resultset_obj->add_avu('fluidigm_plate', '1381735059');
  $resultset_obj->add_avu('fluidigm_well', 'S01');
  $resultset_obj->add_avu('dcterms:identifier', 'ABC0123456789');
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
    (irods => $irods)->get_assay_resultsets('qc', 'ABC0123456789');

  cmp_ok(scalar @resultsets, '==', 1, 'Assay resultsets');
}

1;
