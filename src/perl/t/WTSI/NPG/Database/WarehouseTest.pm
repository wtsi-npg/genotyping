
use utf8;

package WTSI::NPG::Database::WarehouseTest;

use strict;
use warnings;

use base qw(WTSI::NPG::Test);
use Test::More tests => 439;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Database::Warehouse'); }

use WTSI::NPG::Database::Warehouse;

my $db_credentials = $ENV{HOME} . "/.npg/genotyping.ini";

sub require : Test(1) {
  require_ok('WTSI::NPG::Database::Warehouse');
}

sub connect : Test(7) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials);

  is($db->name, 'sequencescape_warehouse', 'Has correct name');
  ok($db->data_source, 'Has a data_source');
  ok($db->username, 'Has a username');

  ok(!$db->is_connected, 'Initially, is not connected');
  ok($db->connect(RaiseError => 1), 'Can connect');
  ok($db->is_connected, 'Is connected');
  ok($db->dbh, 'Has a database handle');
  $db->disconnect;
}

sub disconnect : Test(4) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials);

  ok($db->connect(RaiseError => 1), 'Can connect');
  ok($db->is_connected, 'Is connected');
  ok($db->disconnect, 'Can disconnect');
  ok(!$db->is_connected, 'Finally, is not connected');
}

sub find_plate : Test(195) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials)->connect(RaiseError           => 1,
                                           mysql_enable_utf8    => 1,
                                           mysql_auto_reconnect => 0);

  # {"HEX(uuid)":"6DF312A8E05D11E0A43E68B59976A382",
  # "internal_id":3639150, "name":"Cherrypicked 203043",
  # "barcode":"203043", "barcode_prefix":"DN", "plate_size":96,
  # "is_current":true, "checked_at":"2013-01-15 15:19:42",
  # "last_updated":"2012-10-22 11:40:54", "created":"2011-09-16
  # 12:14:30", "plate_purpose_name":"OMNI2.5M-8",
  # "plate_purpose_internal_id":89,
  # "HEX(plate_purpose_uuid)":"8A9E0F321C4011E2B92568B59976A382",
  # "infinium_barcode":"WG0002161-DNA", "location":"Genotyping
  # freezer", "inserted_at":"2013-01-15 15:19:42",
  # "deleted_at":null, "current_from":"2012-10-22 11:40:54",
  # "current_to":null, "fluidigm_barcode":null}

  my $plate_id = 3639150;
  my $plate = $db->find_plate($plate_id);

  dies_ok {$db->find_plate(undef) } 'Defined plate ID';
  dies_ok {$db->find_plate('')    } 'Non-empty plate ID';

  cmp_ok(scalar keys %$plate, '==', 96, 'Plate size');
  foreach my $well (sort keys %$plate) {
    is($plate->{$well}->{barcode_prefix}, 'DN',     'Plate barcode prefix');
    is($plate->{$well}->{barcode},        '203043', 'Plate barcode');
  }

  $db->disconnect;
}

sub find_sample_by_plate : Test(11) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials)->connect(RaiseError           => 1,
                                           mysql_enable_utf8    => 1,
                                           mysql_auto_reconnect => 0);

  # {"internal_id":1167194, "sanger_sample_id":"TDT5140520",
  # "consent_withdrawn":false,
  # "HEX(sm.uuid)":"7C9BAB969BFF11E0AA53005056A80041",
  # "name":"TDT5140520", "common_name":"Homo Sapien",
  # "supplier_name":"78408", "accession_number":null,
  # "gender":"Female", "cohort":"TDT_GHA", "control":false,
  # "study_internal_id":1207, "barcode_prefix":"DN",
  # "barcode":"203043", "plate_purpose_name":"OMNI2.5M-8",
  # "map":"A1"}

  my $plate_id = 3639150;
  my $well = 'A10';

  dies_ok {$db->find_sample_by_plate(undef, $well) } 'Defined plate ID';
  dies_ok {$db->find_sample_by_plate('',    $well) } 'Non-empty plate ID';
  dies_ok {$db->find_sample_by_plate($plate_id, undef) } 'Defined well';
  dies_ok {$db->find_sample_by_plate($plate_id, '') } 'Non-empty well';

  dies_ok {$db->find_sample_by_plate($plate_id, 'A00') }  'Valid well 1';
  dies_ok {$db->find_sample_by_plate($plate_id, 'A001') } 'Valid well 2';
  dies_ok {$db->find_sample_by_plate($plate_id, 'A100') } 'Valid well 3';

  my $sample = $db->find_sample_by_plate($plate_id, $well);

  cmp_ok($sample->{internal_id}, '==', 1169485, 'Sample internal_id');
  is($sample->{map}, $well, 'Sample map');
  is($sample->{barcode_prefix}, 'DN', 'Plate barcode prefix');
  is($sample->{barcode}, '203043', 'Plate barcode');

  $db->disconnect;
}

sub find_infinium_plate : Test(195) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials)->connect(RaiseError           => 1,
                                           mysql_enable_utf8    => 1,
                                           mysql_auto_reconnect => 0);

  # {"HEX(uuid)":"6DF312A8E05D11E0A43E68B59976A382",
  # "internal_id":3639150, "name":"Cherrypicked 203043",
  # "barcode":"203043", "barcode_prefix":"DN", "plate_size":96,
  # "is_current":true, "checked_at":"2013-01-15 15:19:42",
  # "last_updated":"2012-10-22 11:40:54", "created":"2011-09-16
  # 12:14:30", "plate_purpose_name":"OMNI2.5M-8",
  # "plate_purpose_internal_id":89,
  # "HEX(plate_purpose_uuid)":"8A9E0F321C4011E2B92568B59976A382",
  # "infinium_barcode":"WG0002161-DNA", "location":"Genotyping
  # freezer", "inserted_at":"2013-01-15 15:19:42",
  # "deleted_at":null, "current_from":"2012-10-22 11:40:54",
  # "current_to":null, "fluidigm_barcode":null}

  my $infinium_barcode = 'WG0002161-DNA';
  my $plate = $db->find_infinium_plate($infinium_barcode);

  dies_ok {$db->find_infinium_plate(undef) } 'Defined Infinium barcode';
  dies_ok {$db->find_infinium_plate('')    } 'Non-empty Infinium barcode';

  cmp_ok(scalar keys %$plate, '==', 96, 'Plate size');
  foreach my $well (sort keys %$plate) {
    is($plate->{$well}->{barcode_prefix}, 'DN',     'Plate barcode prefix');
    is($plate->{$well}->{barcode},        '203043', 'Plate barcode');
  }

  $db->disconnect;
}

sub find_infinium_sample_by_plate : Test(11) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials)->connect(RaiseError           => 1,
                                           mysql_enable_utf8    => 1,
                                           mysql_auto_reconnect => 0);

  # {"internal_id":1167194, "sanger_sample_id":"TDT5140520",
  # "consent_withdrawn":false,
  # "HEX(sm.uuid)":"7C9BAB969BFF11E0AA53005056A80041",
  # "name":"TDT5140520", "common_name":"Homo Sapien",
  # "supplier_name":"78408", "accession_number":null,
  # "gender":"Female", "cohort":"TDT_GHA", "control":false,
  # "study_internal_id":1207, "barcode_prefix":"DN",
  # "barcode":"203043", "plate_purpose_name":"OMNI2.5M-8",
  # "map":"A1"}

  my $infinium_barcode = 'WG0002161-DNA';
  my $well = 'A10';

  dies_ok {$db->find_infinium_sample_by_plate(undef, $well) }
    'Defined Infinium barcode';
  dies_ok {$db->find_infinium_sample_by_plate('',    $well) }
    'Non-empty Infinium barcode';
  dies_ok {$db->find_sample_by_plate($infinium_barcode, undef) }
    'Defined well';
  dies_ok {$db->find_infinium_sample_by_plate($infinium_barcode, '') }
    'Non-empty well';

  dies_ok {$db->find_infinium_sample_by_plate($infinium_barcode, 'A00') }
    'Valid well 1';
  dies_ok {$db->find_infinium_sample_by_plate($infinium_barcode, 'A001') }
    'Valid well 2';
  dies_ok {$db->find_infinium_sample_by_plate($infinium_barcode, 'A100') }
    'Valid well 3';

  my $sample = $db->find_infinium_sample_by_plate($infinium_barcode, $well);

  cmp_ok($sample->{internal_id}, '==', 1169485, 'Sample internal_id');
  is($sample->{map}, $well, 'Sample map');
  is($sample->{barcode_prefix}, 'DN', 'Plate barcode prefix');
  is($sample->{barcode}, '203043', 'Plate barcode');

  $db->disconnect;
}

sub find_infinium_gex_sample : Test(8) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials)->connect(RaiseError           => 1,
                                           mysql_enable_utf8    => 1,
                                           mysql_auto_reconnect => 0);

  # {"internal_id":1761128, "sanger_sample_id":"QC1Hip-228",
  # "consent_withdrawn":false,
  # "HEX(sm.uuid)":"5D079C102E8501319F24005056A878E4",
  # "name":"QC1Hip-228", "common_name":"Homo sapiens",
  # "supplier_name":"b4a01ff6-a768-4c1d-8881-143293913dd0",
  # "accession_number":null, "gender":"Unknown", "cohort":null,
  # "control":null, "study_internal_id":2625, "barcode_prefix":"DN",
  # "barcode":"321190", "plate_purpose_name":"Illumina GEX - Hu12v4",
  # "map":"A1"}

  my $plate_barcode = 'DN321190M';
  my $well = 'A1';

  my $sample = $db->find_infinium_gex_sample($plate_barcode, $well);

  dies_ok {$db->find_infinium_gex_sample(undef, $well) }
    'Defined plate barcode';
  dies_ok {$db->find_infinium_gex_sample('',    $well) }
    'Non-empty plate barcode';
  dies_ok {$db->find_sample_gex_sample($plate_barcode, undef) }
    'Defined well';
  dies_ok {$db->find_infinium_gex_sample($plate_barcode, '') }
    'Non-empty well';

  cmp_ok($sample->{internal_id}, '==', 1761128, 'Sample internal_id');
  is($sample->{map}, $well, 'Sample map');
  is($sample->{barcode_prefix}, 'DN', 'Plate barcode prefix');
  is($sample->{barcode}, '321190', 'Plate barcode');

  $db->disconnect;
}

sub find_infinium_gex_sample_by_sanger_id : Test(4) {
  my $db = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile =>  $db_credentials)->connect(RaiseError           => 1,
                                           mysql_enable_utf8    => 1,
                                           mysql_auto_reconnect => 0);

  # {"internal_id":1761128, "sanger_sample_id":"QC1Hip-228",
  # "consent_withdrawn":false,
  # "HEX(sm.uuid)":"5D079C102E8501319F24005056A878E4",
  # "name":"QC1Hip-228", "common_name":"Homo sapiens",
  # "supplier_name":"b4a01ff6-a768-4c1d-8881-143293913dd0",
  # "accession_number":null, "gender":"Unknown", "cohort":null,
  # "control":null, "study_internal_id":2625, "barcode_prefix":"DN",
  # "barcode":"321190", "plate_purpose_name":"Illumina GEX - Hu12v4",
  # "map":"A1"}

  my $sanger_sample_id = 'QC1Hip-228';
  my $well = 'A1';

  my $sample = $db->find_infinium_gex_sample_by_sanger_id
    ($sanger_sample_id);

  cmp_ok($sample->{internal_id}, '==', 1761128, 'Sample internal_id');
  is($sample->{map}, $well, 'Sample map');
  is($sample->{barcode_prefix}, 'DN', 'Plate barcode prefix');
  is($sample->{barcode}, '321190', 'Plate barcode');

  $db->disconnect;
}

sub find_fluidigm_plate : Test(1) {
 TODO: {
    local $TODO = 'Make mock warehouse on SQLite';

    fail('find_fluidigm_plate');
  }
}

sub find_fluidigm_sample_by_plate : Test(1) {
 TODO: {
    local $TODO = 'Make mock warehouse on SQLite';

    fail('find_fluidigm_plate');
  }
}

1;
