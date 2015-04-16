
use utf8;

{
  package WTSI::NPG::Genotyping::Database::InfiniumStub;

  use Moose;

  extends 'WTSI::NPG::Genotyping::Database::Infinium';

  has 'test_chip_design' =>
    (is       => 'rw',
     isa      => 'Str',
     required => 0,
     default  => sub { 'design1' });

  my $root = "./t/query_project_samples";

  sub find_project_samples {
    my ($self, $project_arg) = @_;

    my $project = 'project1';
    if ($project_arg ne $project) {
        die "InfiniumStub expected $project but got $project_arg";
    }
    return
      [ {project           => $project,
         plate             => 'plate1',
         well              => 'A10',
         sample            => 'sample1',
         beadchip          => '012345689',
         beadchip_section  => 'R01C01',
         beadchip_design   => $self->test_chip_design,
         beadchip_revision => '1',
         status            => 'Pass',
         gtc_path          => "$root/sample1.gtc",
         idat_grn_path     => "$root/sample1_Grn.idat",
         idat_red_path     => "$root/sample1_Red.idat"}];
  }

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

{
  package WTSI::NPG::Database::WarehouseStub;

  use warnings;
  use Carp;
  use Moose;

  extends 'WTSI::NPG::Database::Warehouse';

  sub find_infinium_sample_by_plate {
    my ($self, $infinium_barcode, $map) = @_;

    $map eq 'A10' or
       confess "WarehouseStub expected map argument 'A10' but got '$map'";

    return {internal_id        => 123456789,
            sanger_sample_id   => '0123456789',
            consent_withdrawn  => 0,
            donor_id           => 'D999',
            uuid               => 'AAAAAAAAAABBBBBBBBBBCCCCCCCCCCDD',
            name               => 'sample1',
            common_name        => 'Homo sapiens',
            supplier_name      => 'aaaaaaaaaa',
            accession_number   => 'A0123456789',
            gender             => 'Female',
            cohort             => 'AAA111222333',
            control            => 'XXXYYYZZZ',
            study_id           => 0,
            barcode_prefix     => 'DN',
            barcode            => '0987654321',
            plate_purpose_name => 'Infinium',
            map                => 'A10'};
  }

  __PACKAGE__->meta->make_immutable;

  no Moose;

  1;
}

package WTSI::NPG::Genotyping::Infinium::SampleQueryTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 6;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::Infinium::SampleQuery') };

my $config = $ENV{HOME} . "/.npg/genotyping.ini";

my $data_path = './t/query_project_samples';
my @data_file_names = ("sample1_Grn.idat",
                       "sample1_Red.idat",
                       "sample1.gtc");
my @data_files;
foreach my $name (@data_file_names) { 
    push @data_files, $data_path."/".$name;
}

# want to test: get_infinium_data, get_irods_metadata, get_warehouse_data
# use temporary irods collection, infinium & warehouse stubs

my $project = 'project1';
my $irods_tmp_coll;
my $pid = $$;

# Database handle stubs
my $ifdb;
my $ssdb;

my @expected_infinium_data = ([ 'plate1',
                                'A10',
                                'sample1',
                                '012345689',
                                'R01C01',
                                ''
                            ]);

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SampleQueryTest.$pid");

  $ifdb = WTSI::NPG::Genotyping::Database::InfiniumStub->new
    (name    => 'infinium',
     inifile => $config)->connect(RaiseError => 1);

  $ssdb = WTSI::NPG::Database::WarehouseStub->new
    (name    => 'sequencescape_warehouse',
     inifile => File::Spec->catfile($ENV{HOME}, '.npg/genotyping.ini'));

};


sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::Infinium::SampleQuery');
}


sub construct : Test(1) {
    new_ok('WTSI::NPG::Genotyping::Infinium::SampleQuery',
           ['infinium_db' => $ifdb,
            'sequencescape_db' => $ssdb]);
}

sub test_infinium : Test(1) {
    # test of _get_infinium_data
    my $sample_query = WTSI::NPG::Genotyping::Infinium::SampleQuery->new(
        'infinium_db' => $ifdb,
        'sequencescape_db' => $ssdb
    );
    my @data = $sample_query->_find_infinium_data($project);
    is_deeply(\@data, \@expected_infinium_data, 'Infinium data OK');
}

sub test_irods : Test(1) {
    # test of _get_irods_metadata
    # first, add dummy files to the test iRODS
    my $irods = WTSI::NPG::iRODS->new;
    my @attributes = qw/infinium_plate infinium_well infinium_sample
                        beadchip beadchip_section/;
    foreach my $name (@data_file_names) {
        $irods->add_object($data_path.'/'.$name, $irods_tmp_coll);
        my $irods_obj = $irods_tmp_coll.'/'.$name;
        for (my $i=0;$i<@attributes;$i++) {
            $irods->add_object_avu($irods_obj,
                                   $attributes[$i],
                                   $expected_infinium_data[0][$i]);
        }
    }
    # now run query on the test iRODS
    my $sample_query = WTSI::NPG::Genotyping::Infinium::SampleQuery->new(
        'infinium_db' => $ifdb,
        'sequencescape_db' => $ssdb
    );
    my @irods_metadata = $sample_query->_find_irods_metadata
        (\@expected_infinium_data, $irods_tmp_coll);
    is_deeply(\@irods_metadata, \@expected_infinium_data, 'iRODS metadata OK');
}

sub test_warehouse : Test(1) {
    # test of _get_warehouse_data
    my $sample_query = WTSI::NPG::Genotyping::Infinium::SampleQuery->new(
        'infinium_db' => $ifdb,
        'sequencescape_db' => $ssdb
    );
    my @wh_data = $sample_query->_find_warehouse_data(\@expected_infinium_data);
    my @expected_wh_data = (['plate1',
                             'A10',
                             'sample1',
                             '',
                             '',
                             '0987654321'
                         ]);
    is_deeply(\@wh_data, \@expected_wh_data, "Warehouse data OK");
}

1;
