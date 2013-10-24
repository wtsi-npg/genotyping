
use utf8;

package WTSI::NPG::Genotyping::PublicationTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 3;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

use WTSI::NPG::Genotyping::Database::Pipeline;

use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri);

use WTSI::NPG::Genotyping::Publication qw(publish_analysis_directory);

use WTSI::NPG::iRODS qw(add_collection
                        add_object_meta
                        find_collections_by_meta
                        find_objects_by_meta
                        list_collection
                        put_collection
                        remove_collection);

my $sample_data_path = './t/publish_analysis_directory/data/infinium';
my $analysis_data_path = './t/publish_analysis_directory/data/analysis';
my $pipeline_dbfile = "$analysis_data_path/genotyping.db";
my $irods_tmp_coll;
my $test_genotyping_project = 'test_project';

my $pid = $$;

sub make_fixture : Test(setup) {
  $irods_tmp_coll = add_collection("PublicationTest.$pid");
  put_collection($sample_data_path, $irods_tmp_coll);

  my @data_files = qw(1111111111_R01C01.gtc
                      1111111111_R01C01_Grn.idat
                      1111111111_R01C01_Red.idat
                      2222222222_R01C01.gtc
                      2222222222_R01C01_Grn.idat
                      2222222222_R01C01_Red.idat
                      3333333333_R01C01.gtc
                      3333333333_R01C01_Grn.idat
                      3333333333_R01C01_Red.idat);

  my @sample_ids = qw(name1
                      name1
                      name1
                      name2
                      name2
                      name2
                      name3
                      name3
                      name3);
  my $study_id = 0;

  for (my $i = 0; $i < scalar @data_files; $i++) {
    my $irods_path = "$irods_tmp_coll/infinium/" . $data_files[$i];
    my $sample_id = $sample_ids[$i];
    add_object_meta($irods_path, 'dcterms:title', $test_genotyping_project);
    add_object_meta($irods_path, 'dcterms:identifier', $sample_id);
    add_object_meta($irods_path, 'study_id', $study_id);
  }
}

sub teardown : Test(teardown) {
  remove_collection($irods_tmp_coll);
  unlink $pipeline_dbfile;
}

sub publish : Test(3) {
  my $publish_dest = $irods_tmp_coll;
  my $sample_archive = "$irods_tmp_coll/infinium";
  my $run_name = 'test';
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $time = DateTime->now;
  my $pipedb = make_pipedb($pipeline_dbfile);

  my $analysis_uuid =
    publish_analysis_directory($analysis_data_path,
                               $creator_uri, $publish_dest,
                               $publisher_uri, $pipedb, $run_name,
                               $sample_archive, $time);

  ok($analysis_uuid, "Yields analysis UUID");
  my @analysis_data =
    find_collections_by_meta($irods_tmp_coll,
                             [analysis_uuid => $analysis_uuid]);
  cmp_ok(scalar @analysis_data, '==', 1, "A single analysis annotated");

  my @sample_data = find_objects_by_meta("$irods_tmp_coll/infinium",
                                         [analysis_uuid => $analysis_uuid]);
  my @expected_sample_data = map { "$irods_tmp_coll/infinium/$_" }
     qw(1111111111_R01C01.gtc
        1111111111_R01C01_Grn.idat
        1111111111_R01C01_Red.idat
        2222222222_R01C01.gtc
        2222222222_R01C01_Grn.idat
        2222222222_R01C01_Red.idat);

  is_deeply(\@sample_data, \@expected_sample_data,
           "Annotated sample objects match") or diag explain \@sample_data;
}

sub make_pipedb {
  my ($dbfile) = @_;

  my $config = $ENV{HOME} . "/.npg/genotyping.ini";
  my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
    (name => 'pipeline',
     inifile => $config,
     dbfile => $dbfile,
     overwrite => 1)->connect
       (RaiseError => 1,
        sqlite_unicode => 1,
        on_connect_do => 'PRAGMA foreign_keys = ON')->populate;

  my $snpset = $pipedb->snpset->find({name => 'HumanExome-12v1'});
  my $autocall = $pipedb->method->find({name => 'Autocall'});
  my $withdrawn = $pipedb->state->find({name => 'withdrawn'});

  $pipedb->in_transaction
    (sub {
       my $supplier = $pipedb->datasupplier->find_or_create
         ({name => 'publication_test',
           namespace => 'wtsi'});
       my $run = $pipedb->piperun->find_or_create({name => 'test'});
       my $dataset = $run->add_to_datasets
         ({if_project => $test_genotyping_project,
           datasupplier => $supplier,
           snpset => $snpset});
       foreach my $i (1..3) {
         my $beadchip = $i x 10;
         my @args = ("name$i", "sample$i", $beadchip, $autocall,
                     "$sample_data_path/" . $beadchip . "_R01C01.gtc",
                     "$sample_data_path/" . $beadchip . "_R01C01_Red.idat",
                     "$sample_data_path/" . $beadchip . "_R01C01_Grn.idat");

         my $sample = add_sample($dataset, @args);

         if ($i == 3) {
           $sample->add_to_states($withdrawn);
           $sample->include_from_state;
           $sample->update;
         }
       }
     });

  return $pipedb;
}

sub add_sample {
  my ($dataset, $name, $id, $beadchip, $method, $gtc, $red, $grn) = @_;

  my $sample = $dataset->add_to_samples({name => $name,
                                         sanger_sample_id => $id,
                                         beadchip => $beadchip,
                                         include => 1,
                                         supplier_name => 'test_supplier_name',
                                         rowcol => 'R01C01'});
  $sample->add_to_results({method => $method, value => $gtc});
  $sample->add_to_results({method => $method, value => $red});
  $sample->add_to_results({method => $method, value => $grn});

  return $sample;
}
