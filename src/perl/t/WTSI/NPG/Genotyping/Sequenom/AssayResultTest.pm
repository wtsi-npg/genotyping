
use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayResultTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 3;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::AssayResult'); }

use WTSI::NPG::Genotyping::Sequenom::AssayDataObject;
use WTSI::NPG::Genotyping::Sequenom::AssayResult;

my $data_path = './t/sequenom_assay_data_object';
my $data_file = 'plate1_A01.csv';
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SequenomAssayResultTest.$pid");
  my $irods_path = "$irods_tmp_coll/$data_file";

  $irods->add_object("$data_path/$data_file", $irods_path);
  $irods->add_object_avu($irods_path, 'sequenom_plate', 'plate1');
  $irods->add_object_avu($irods_path, 'sequenom_well', 'A01');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::AssayResult');
}

sub constructor : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;

  my $data_object = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new
    ($irods, "$irods_tmp_coll/$data_file");

  new_ok('WTSI::NPG::Genotyping::Sequenom::AssayResult',
         [allele        => 'C',
          assay_id      => 'assay1',
          chip          =>  '1234',
          customer      =>  'customer1',
          experiment    => 'experiment1',
          genotype_id   => 'CT',
          height        => 10,
          mass          => 1,
          plate         => 'plate1',
          project       => 'project1',
          sample_id     => 'sample1',
          status        => 'status1',
          well_position => 'A01',
          str           =>  join("\t", 'C', 'assay1', '1234', 'customer1',
                                 'experiment1', 'CT', 10, 1, 'plate1',
                                 'project1', 'sample1', 'status1', 'A01')]);
}

1;
