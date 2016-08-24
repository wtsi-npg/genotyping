use utf8;

package WTSI::NPG::ArchivableTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Archive::Tar;
use Cwd qw(abs_path);
use DateTime;
use File::Spec;
use File::Temp qw(tempdir);
use Test::More tests => 12;
use Test::Exception;

{
  package WTSI::NPG::Utilities::TestArchiver;

  use Moose;

  with 'WTSI::NPG::Utilities::Archivable';

  has 'dir_regex' =>
      (is       => 'ro',
       isa      => 'RegexpRef',
       default  => sub { return qr/^\d{5}$/msx; }
   );

  has 'output_prefix' => (
      is  => 'ro',
      isa => 'Str',
      default => 'test',
    );

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

use Log::Log4perl;
Log::Log4perl::init('./etc/log4perl_tests.conf');
our $log = Log::Log4perl->get_logger();

BEGIN { use_ok('WTSI::NPG::Utilities::TestArchiver'); }

my $tempdir;
my $data_path = './t/archiver';
my $output_prefix = 'test';

sub make_fixture : Test(setup) {
    $tempdir = tempdir( "ArchivableTest_XXXXXX", CLEANUP => 1);
    $tempdir = abs_path($tempdir);
    my @dirnames = qw/12345 13579 24680 1234567890/;
    foreach my $name (@dirnames) {
        my $dirpath = File::Spec->catfile($tempdir, $name);
        mkdir($dirpath);
        `cp $data_path/lorem.txt $dirpath`;
        if ($name eq '12345' || $name eq '13579' || $name eq '1234567890') {
            `touch $dirpath -d 2000-01-01`; # change modification time
            `touch $dirpath/lorem.txt -d 2000-01-01`;
        }
    }
}

sub add_to_archives : Test(8) {
    my $archiver = WTSI::NPG::Utilities::TestArchiver->new(
        output_dir => $tempdir,
    );
    my @files = ( File::Spec->catfile($tempdir, '12345', 'lorem.txt'),
                  File::Spec->catfile($tempdir, '13579', 'lorem.txt'),
              );
    my @archive_files = $archiver->add_to_archives(\@files);
    my $expected_archive = File::Spec->catfile($tempdir,
                                               'test_2000-01.tar.gz');
    is_deeply(\@archive_files, [$expected_archive, ],
       "Archive file path matches expected value");
    my $archive_file = $archive_files[0];
    ok(-e $archive_file, "Archive file '$archive_file' exists");

    my $tar = Archive::Tar->new();
    $tar->read($archive_file);
    my @contents = $tar->list_files();
    for (my $i=0;$i<@contents;$i++) {
        # tar removes the leading / from paths in its listing
        is("/".$contents[$i], $files[$i],
           "File ".$files[$i]." appears in archive contents");
    }
    # test appending to an existing archive
    my $extra_file = File::Spec->catfile($tempdir, '24680', 'lorem.txt');
    `touch -d 2000-01-01 $extra_file`;
    ok($archiver->add_to_archives([$extra_file]),
       'Extra file added to archive');
    $tar->read($archive_file);
    @contents = $tar->list_files();
    push(@files, $extra_file);
    for (my $i=0;$i<@contents;$i++) {
        # tar removes the leading / from paths in its listing
        is("/".$contents[$i], $files[$i],
           "File ".$files[$i]." appears in updated archive contents");
    }
}

sub find_directories_to_archive : Test(1) {
    my $archiver = WTSI::NPG::Utilities::TestArchiver->new(
        output_dir => $tempdir,
        target_dir => $tempdir,
        days_ago => 30,
    );
    my @found_dirs = $archiver->find_directories_to_archive();
    my @expected_dirs = (File::Spec->catfile($tempdir, '12345'),
                         File::Spec->catfile($tempdir, '13579'));
    is_deeply(\@found_dirs, \@expected_dirs,
              "Candidate directories for archiving match expected values");
}

sub monthly_archive_name : Test(1) {
    my $dt = DateTime->new(
        year       => 2016,
        month      => 4,
        day        => 1,
    );
    my $time = $dt->epoch;
    my $archiver = WTSI::NPG::Utilities::TestArchiver->new(
        output_dir => $tempdir,
    );
    is($archiver->monthly_archive_filename($time), 'test_2016-04.tar.gz',
       'Monthly archive filename matches expected value');

}

sub output_directory : Test(1) {
    my $output_dir = "/dummy/nonexistent/directory";
    dies_ok {
        WTSI::NPG::Utilities::TestArchiver->new(output_dir => $output_dir);
    }, "Dies on nonexistent output directory";
}

1;
