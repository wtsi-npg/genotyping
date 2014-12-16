
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResultSetTest;

use strict;
use warnings;

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 68;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet'); }

use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;

my $data_path = './t/fluidigm_assay_data_object/1381735059';
my $data_file = 'S01_1381735059.csv';
my $resultset;

my $snpset_path = './t/fluidigm_assay_data_object/';
my $snpset_file = 'qc.tsv';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("FluidigmAssayResultSetTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/1381735059/$data_file";

  $irods->add_object_avu($irods_path, 'fluidigm_plate', '1381735059');
  $irods->add_object_avu($irods_path, 'fluidigm_well', 'S01');
  $irods->add_object_avu($irods_path, 'fluidigm_plex', 'qc');

  $irods->add_object("$snpset_path/$snpset_file",
                     "$irods_tmp_coll/$snpset_file");
  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/$snpset_file")->absolute;
  $snpset_obj->add_avu('fluidigm_plex', 'qc');
  $snpset_obj->add_avu('reference_name', 'Homo_sapiens (1000Genomes)');

  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, $irods_path);
  $resultset = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
    ($data_object);
}

sub teardown : Test(teardown) {
  undef $resultset;

  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet');
}

sub constructor : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
    ($irods, "$irods_tmp_coll/1381735059/$data_file");

  # From file
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         [file_name => "$data_path/$data_file"]);
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         ["$data_path/$data_file"]);

  # From data object
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         [data_object => $data_object]);
  new_ok('WTSI::NPG::Genotyping::Fluidigm::AssayResultSet',
         [$data_object]);

  dies_ok {
    WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new
        (file_name   => "$data_path/$data_file",
         data_object => $data_object);
  } 'Cannot construct from both file and data object';
}


sub size : Test(1) {
  cmp_ok($resultset->size, '==', 96, 'Expected size');
}

sub assay_results : Test(1) {
  cmp_ok(scalar @{$resultset->assay_results}, '==', 96,
         'Contains expected number of assay results');
}

sub assay_addresses : Test(1) {
  my @expected_addresses = map { sprintf("A%02d", $_) } (1 .. 96);
  my @addresses = @{$resultset->assay_addresses};
  is_deeply(\@addresses, \@expected_addresses,
             'Contains expected assay addresses') or diag explain \@addresses;
}

sub sample_name : Test(1) {
  is($resultset->sample_name, 'ABC0123456789', 'Correct sample name');
}

sub snpset_name : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;

  is($resultset->snpset_name, 'qc', 'Correct SNP set name');

  $irods->add_object_avu($resultset->data_object->str,
                         'fluidigm_plex', 'test');
  $resultset->data_object->clear_metadata; # Clear metadata cache

  dies_ok { $resultset->snpset_name }, 'Cannot have multiple names';
}

sub snp_names : Test(1) {
  my @snp_names = @{$resultset->snp_names};
  my @expected_names = qw(GS34251
                          GS35220
                          rs11096957
                          rs12828016
                          rs156697
                          rs1801262
                          rs1805034
                          rs1805087
                          rs2247870
                          rs2286963
                          rs3742207
                          rs3795677
                          rs4075254
                          rs4619
                          rs4843075
                          rs5215
                          rs6166
                          rs649058
                          rs6557634
                          rs6759892
                          rs7298565
                          rs753381
                          rs7627615
                          rs8065080);

  is_deeply(\@snp_names, \@expected_names,
            'Contains expected SNP names') or diag explain \@snp_names;
}

sub filter_on_confidence : Test(1) {
  cmp_ok(scalar @{$resultset->filter_on_confidence(100)}, '==', 23,
         'Filter on confidence')
}

sub result_at : Test(53) {
  my $irods = WTSI::NPG::iRODS->new;
  my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/$snpset_file")->absolute;
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new(data_object => $snpset_obj);

  my @expected_calls = (['GS34251',    'TC'],
                        ['GS34251',    'TC'],
                        ['GS35220',    'CT'],
                        ['GS35220',    'CT'],
                        ['rs11096957', 'TG'],
                        ['rs12828016', 'GT'],
                        ['rs156697',   'AG'],
                        ['rs1801262',  'TC'],
                        ['rs1805034',  'CT'],
                        ['rs1805087',  'AG'],
                        ['rs2247870',  'GA'],
                        ['rs2286963',  'TG'],
                        ['rs3742207',  'TG'],
                        ['rs3795677',  'GA'],
                        ['rs4075254',  'GA'],
                        ['rs4619',     'AG'],
                        ['rs4843075',  'GA'],
                        ['rs5215',     'CT'],
                        ['rs6166',     'CT'],
                        ['rs649058',   'GA'],
                        ['rs6557634',  'TC'],
                        ['rs6759892',  'TG'],
                        ['rs7298565',  'GA'],
                        ['rs753381',   'TC'],
                        ['rs7627615',  'GA'],
                        ['rs8065080',  'TC']);

  my @observed_calls;
  foreach my $position (1 .. 26) {
    my $address = sprintf("A%02d", $position);
    my $result = $resultset->result_at($address);
    is($result->sample_address, "S01", "Sample address $position");
    is($result->assay_address, $address, "Assay address $position");

    push @observed_calls, [$result->snp_assayed, $result->canonical_call];
  }

  is_deeply(\@observed_calls, \@expected_calls,
            'Contains expected SNP names') or diag explain \@observed_calls;
}

1;
