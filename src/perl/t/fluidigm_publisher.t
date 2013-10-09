
# Tests WTSI::NPG::Genotyping::FluidigmPublisher

use utf8;

use strict;
use warnings;
use DateTime;

use Test::More tests => 3;
use Test::Exception;

use WTSI::NPG::Genotyping::FluidigmExportFile;

use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri);

Log::Log4perl::init('etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::FluidigmPublisher'); }
require_ok('WTSI::NPG::Genotyping::FluidigmPublisher');

my $data_path = './t/fluidigm_export_file';
my $complete_file = "$data_path/complete.csv";
my $export = WTSI::NPG::Genotyping::FluidigmExportFile->new
  ({file_name=> $complete_file});

my $uid = `whoami`;
chomp($uid);
my $creator_uri = get_wtsi_uri();
my $publisher_uri = get_publisher_uri($uid);
my $publication_time = DateTime->now();

ok(WTSI::NPG::Genotyping::FluidigmPublisher->new
   (creator_uri => $creator_uri,
    publisher_uri => $publisher_uri,
    publication_time => $publication_time,
    fluidigm_export => $export));

my $publisher = WTSI::NPG::Genotyping::FluidigmPublisher->new
   (creator_uri => $creator_uri,
    publisher_uri => $publisher_uri,
    publication_time => $publication_time,
    fluidigm_export => $export);

ok($publisher->publish('test'));
