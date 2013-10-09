use utf8;

package WTSI::NPG::Genotyping::FluidigmPublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(Test::Class);
use Test::More tests => 5;
use Test::Exception;

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmPublisher') };

use WTSI::NPG::Genotyping::FluidigmPublisher;
use WTSI::NPG::Genotyping::FluidigmExportFile;

use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri);

use WTSI::NPG::iRODS qw(find_objects_by_meta);

my $data_path = './t/fluidigm_export_file';
my $complete_file = "$data_path/complete.csv";

my $export_file;
my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  $export_file = WTSI::NPG::Genotyping::FluidigmExportFile->new
    (file_name=> $complete_file);

  $irods_tmp_coll = "FluidigmPublisherTest.$pid";
  system("imkdir", $irods_tmp_coll) == 0
    or die "Failed to create iRODS temp collection $irods_tmp_coll\n";
};

sub teardown : Test(teardown) {
  undef $export_file;

  system("irm", "-r", "-f", $irods_tmp_coll) == 0
    or die "Failed to remove iRODS temp collection $irods_tmp_coll\n";
};

sub require : Test {
  require_ok('WTSI::NPG::Genotyping::FluidigmPublisher');
};

sub constructor : Test {
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  new_ok('WTSI::NPG::Genotyping::FluidigmPublisher',
         [creator_uri => $creator_uri,
          publisher_uri => $publisher_uri,
          publication_time => $publication_time,
          fluidigm_export => $export_file]);
};

sub publish : Test(2) {
  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $publication_time = DateTime->now();

  my $publisher = WTSI::NPG::Genotyping::FluidigmPublisher->new
    (creator_uri => $creator_uri,
     publisher_uri => $publisher_uri,
     publication_time => $publication_time,
     fluidigm_export => $export_file);

  my @addresses_to_publish = qw(S01 S02 S03 S04 S05);
  my $num_published = $publisher->publish($irods_tmp_coll,
                                          @addresses_to_publish );

  cmp_ok($num_published, '==', scalar @addresses_to_publish,
         "Number of chunks published");

  my @data_objects = find_objects_by_meta($irods_tmp_coll,
                                          ['fluidigm_plate' => '1381735059']);
  cmp_ok(@data_objects, '==', scalar @addresses_to_publish,
         "Number of chunks published with fluidigm_plate metadata");
};
