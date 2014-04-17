
use utf8;

{
  package WTSI::NPG::Genotyping::Database::SequenomStub;

  use strict;
  use warnings;

  use base 'WTSI::NPG::Genotyping::Database::Sequenom';

  sub find_plate_results {
    return {'A01' => [{customer   => 'customer1',
                       project    => 'project1',
                       plate      => 'plate1',
                       experiment => 'experiment1',
                       chip       => 1234,
                       well       => 'A01',
                       assay      => 'assay1-rs012345678',
                       genotype   => 'CT',
                       status     => 'status1',
                       sample     => 'sample1',
                       allele     => 'C',
                       mass       => 1,
                       height     => 10},
                      {customer   => 'customer1',
                       project    => 'project1',
                       plate      => 'plate1',
                       experiment => 'experiment1',
                       chip       => 1234,
                       well       => 'A01',
                       assay      => 'assay1-rs987654321',
                       genotype   => 'CT',
                       status     => 'status1',
                       sample     => 'sample1',
                       allele     => 'C',
                       mass       => 1,
                       height     => 10}]};
  }
}

package WTSI::NPG::Genotyping::Sequenom::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 7;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Sequenom::Publisher') };

use WTSI::NPG::Genotyping::Sequenom::Publisher;
use WTSI::NPG::iRODS;

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = "SequenomPublisherTest.$pid";
  $irods->add_collection($irods_tmp_coll);
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Sequenom::Publisher');
}

sub constructor : Test(1) {
  my $publication_time = DateTime->now;
  my $plate_name = 'plate1';

  my $sqdb = WTSI::NPG::Genotyping::Database::SequenomStub->new
    (name => 'mspec2',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  new_ok('WTSI::NPG::Genotyping::Sequenom::Publisher',
         [publication_time => $publication_time,
          plate_name       => $plate_name,
          sequenom_db      => $sqdb]);
};

sub publish : Test(2) {
  my $publication_time = DateTime->now;
  my $plate_name = 'plate1';

  my $sqdb = WTSI::NPG::Genotyping::Database::SequenomStub->new
    (name => 'mspec2',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $publisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
    (publication_time => $publication_time,
     plate_name       => $plate_name,
     sequenom_db      => $sqdb);

  my @addresses_to_publish = qw(A01);
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', scalar @addresses_to_publish,
         "Number of wells published");

  my $irods = WTSI::NPG::iRODS->new;
  my @published_data =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [sequenom_plate => 'plate1'],
                                 [sequenom_well  => 'A01'],
                                 [sequenom_plex  => 'assay1']);
  cmp_ok(scalar @published_data, '==', scalar @addresses_to_publish,
         "Number of wells published with sequenom_plate metadata");
}

sub publish_overwrite : Test(2) {
  my $publication_time = DateTime->now;
  my $plate_name = 'plate1';

  my $sqdb = WTSI::NPG::Genotyping::Database::SequenomStub->new
    (name => 'mspec2',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

  my $publisher = WTSI::NPG::Genotyping::Sequenom::Publisher->new
    (publication_time => $publication_time,
     plate_name       => $plate_name,
     sequenom_db      => $sqdb);

  my @addresses_to_publish = qw(A01);
  # Publish
  $publisher->publish($irods_tmp_coll, @addresses_to_publish);
  # Publishing again should be a no-op
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish);
  cmp_ok($num_published, '==', 1, "Number of wells published");

  my $irods = WTSI::NPG::iRODS->new;
  my @data_objects =
    $irods->find_objects_by_meta($irods_tmp_coll,
                                 [sequenom_plate => 'plate1'],
                                 [sequenom_well  => 'A01'],
                                 [sequenom_plex  => 'assay1'] );
  cmp_ok(scalar @data_objects, '==', scalar @addresses_to_publish,
         "Number of wells published with sequenom_plate metadata");
}
