
use utf8;

package WTSI::NPG::Genotyping::Infinium::AnalysisPublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 5;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Infinium::AnalysisPublisher') };

use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::Infinium::AnalysisPublisher;
use WTSI::NPG::iRODS;

my $data_path = './t/infinium_analysis_publisher/data';
my $sample_data_path = "$data_path/samples/infinium";
my $analysis_data_path = "$data_path/analysis";
my $pipeline_dbfile = "$analysis_data_path/genotyping.db";
my $genotyping_project = 'test_project';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll =
    $irods->add_collection("InfiniumAnalysisPublisherTest.$pid");
  $irods->put_collection($sample_data_path, $irods_tmp_coll);

  my @data_files = qw(1111111111_R01C01.gtc
                      1111111111_R01C01_Grn.idat
                      1111111111_R01C01_Red.idat
                      1111111111_R01C02.gtc
                      1111111111_R01C02_Grn.idat
                      1111111111_R01C02_Red.idat
                      2222222222_R01C01.gtc
                      2222222222_R01C01_Grn.idat
                      2222222222_R01C01_Red.idat);

  my @sample_ids = qw(name1 name1 name1
                      name2 name2 name2
                      name3 name3 name3);

  my @beadchips = qw(1111111111 1111111111 1111111111
                     1111111111 1111111111 1111111111
                     2222222222 2222222222 2222222222);

  my @sections = qw(R01C01 R01C01 R01C01
                    R01C02 R01C02 R01C02
                    R01C01 R01C01 R01C01);

  my $study_id = 0;

  for (my $i = 0; $i < scalar @data_files; $i++) {
    my $irods_path = "$irods_tmp_coll/infinium/" . $data_files[$i];
    my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $irods_path)->absolute;
    $obj->add_avu('beadchip', $beadchips[$i]);
    $obj->add_avu('beadchip_section', $sections[$i]);
    $obj->add_avu('dcterms:title', $genotyping_project);
    $obj->add_avu('infinium_sample', $sample_ids[$i]);
    $obj->add_avu('study_id', $study_id);
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
  unlink $pipeline_dbfile;
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Infinium::AnalysisPublisher');
};

sub publish : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;
  my $publish_dest = $irods_tmp_coll;
  my $sample_archive = "$irods_tmp_coll/infinium";
  my $run_name = 'test';
  my $publication_time = DateTime->now;

  my $pipedb = make_pipedb($pipeline_dbfile);

  my $publisher = WTSI::NPG::Genotyping::Infinium::AnalysisPublisher->new
    (analysis_directory => $analysis_data_path,
     pipe_db            => $pipedb,
     publication_time   => $publication_time,
     run_name           => $run_name,
     sample_archive     => $sample_archive);

  my $analysis_uuid = $publisher->publish($publish_dest);
  ok($analysis_uuid, "Yields analysis UUID");

  my @analysis_data =
    $irods->find_collections_by_meta($irods_tmp_coll,
                                     [analysis_uuid => $analysis_uuid]);
  cmp_ok(scalar @analysis_data, '==', 1, "A single analysis annotated");

  my @sample_data =
    $irods->find_objects_by_meta("$irods_tmp_coll/infinium",
                                 [analysis_uuid => $analysis_uuid]);
  # The third sample is excluded from publication
  my @expected_sample_data = map { "$irods_tmp_coll/infinium/$_" }
    qw(1111111111_R01C01.gtc
       1111111111_R01C01_Grn.idat
       1111111111_R01C01_Red.idat
       1111111111_R01C02.gtc
       1111111111_R01C02_Grn.idat
       1111111111_R01C02_Red.idat);

  is_deeply(\@sample_data, \@expected_sample_data,
           "Annotated sample objects match") or diag explain \@sample_data;
}

sub make_pipedb {
  my ($dbfile) = @_;

  my $config = $ENV{HOME} . "/.npg/genotyping.ini";
  my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name      => 'pipeline',
     inifile   => $config,
     dbfile    => $dbfile,
     overwrite => 1)->connect
       (RaiseError     => 1,
        sqlite_unicode => 1,
        on_connect_do  => 'PRAGMA foreign_keys = ON')->populate;

  my $snpset   = $pipedb->snpset->find({name => 'HumanExome-12v1'});
  my $autocall = $pipedb->method->find({name => 'Autocall'});
  my $excluded = $pipedb->state->find({name => 'excluded'});

  $pipedb->in_transaction
    (sub {
       my $supplier = $pipedb->datasupplier->find_or_create
         ({name      => 'publication_test',
           namespace => 'wtsi'});
       my $run = $pipedb->piperun->find_or_create({name => 'test'});
       my $dataset = $run->add_to_datasets
         ({if_project   => $genotyping_project,
           datasupplier => $supplier,
           snpset       => $snpset});

       my @beadchips = qw(1111111111 1111111111 2222222222);
       my @sections  = qw(R01C01 R01C02 R01C01);

       foreach my $i (0..2) {
         my $x = $i + 1;
         my $beadchip = $beadchips[$i];
         my $section  = $sections[$i];

         my @args = ("name$x", "sample$x", $beadchip, $section, $autocall,
                     "$sample_data_path/$beadchip" . "_$section" . ".gtc",
                     "$sample_data_path/$beadchip" . "_$section" . "_Red.idat",
                     "$sample_data_path/$beadchip" . "_$section" . "_Grn.idat");

         my $sample = add_sample($dataset, @args);

         # Exclude the third sample from publication
         if ($i == 2) {
           $sample->add_to_states($excluded);
           $sample->include_from_state;
           $sample->update;
         }
       }
     });

  return $pipedb;
}

sub add_sample {
  my ($dataset, $name, $id, $beadchip, $section, $method,
      $gtc, $red, $grn) = @_;

  my $sample = $dataset->add_to_samples
    ({name             => $name,
      sanger_sample_id => $id,
      beadchip         => $beadchip,
      rowcol           => $section,
      include          => 1,
      supplier_name    => 'test_supplier_name'});
  $sample->add_to_results({method => $method, value => $gtc});
  $sample->add_to_results({method => $method, value => $red});
  $sample->add_to_results({method => $method, value => $grn});

  return $sample;
}
