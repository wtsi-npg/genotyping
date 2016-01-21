
use utf8;

package WTSI::NPG::Genotyping::SNPSetTest;

use strict;
use warnings;

use File::Compare;
use File::Temp qw(tempdir tempfile);
use List::AllUtils qw(all);

use base qw(WTSI::NPG::Test);
use File::Spec;
use Test::More tests => 50;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::SNPSet'); }

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

my $data_path = './t/snpset';
my $data_file = 'qc.tsv';
my $data_file_renamed = 'qc_renamed_snp.tsv';
my $data_file_2 = 'W30467.tsv';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SNPSetTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);
  my $irods_path = "$irods_tmp_coll/snpset/$data_file";
  $irods->add_object_avu($irods_path, 'snpset', 'qc');
  $irods_path = "$irods_tmp_coll/snpset/$data_file_2";
  $irods->add_object_avu($irods_path, 'snpset', 'qc2');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::SNPSet');
}

sub constructor : Test(7) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  # Empty file (WTSI::NPG::iRODS::Storable requires that the file exists)
  my $fh = File::Temp->new;
  new_ok('WTSI::NPG::Genotyping::SNPSet',
         [file_name => $fh->filename]);
  close $fh;

  # From file
  new_ok('WTSI::NPG::Genotyping::SNPSet',
         [file_name => "$data_path/$data_file"]);
  new_ok('WTSI::NPG::Genotyping::SNPSet',
         ["$data_path/$data_file"]);

  # From data object
  new_ok('WTSI::NPG::Genotyping::SNPSet',
         [data_object => $data_object]);
  new_ok('WTSI::NPG::Genotyping::SNPSet',
         [$data_object]);

  dies_ok {
    WTSI::NPG::Genotyping::SNPSet->new
        (file_name   => "$data_path/$data_file",
         data_object => $data_object);
  } 'Cannot construct from both file and data object';

  my $orphan_file = 'qc_orphan_marker.tsv';

  dies_ok {
    WTSI::NPG::Genotyping::SNPSet->new
        (file_name   => "$data_path/$orphan_file");
  } 'Cannot construct from file containing orphan marker records';
}

sub de_novo : Test(3) {
  my $from_file = WTSI::NPG::Genotyping::SNPSet->new
    (file_name => "$data_path/$data_file");

  # Empty file (WTSI::NPG::iRODS::Storable requires that the file exists)
  my $fh = File::Temp->new;
  my $file_name = $fh->filename;
  my $de_novo = WTSI::NPG::Genotyping::SNPSet->new
    (file_name => $file_name,
     snps      => $from_file->snps); # Set some SNPS to write

  ok((all { $_->snpset->contains_snp($_->name) } @{$de_novo->snps}),
     'All SNPs and contained by parent');

  ok($de_novo->write_snpset_data, 'De novo SNPSet written');
  close $fh;

  my $expected_file = "$data_path/$data_file";
  ok(compare($file_name, $expected_file) == 0,
     "$file_name is identical to $expected_file");
}

sub snps : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  cmp_ok(scalar @{$snpset->snps}, '==', 24,
         'Contains expected number of SNPs');

  ok((all { $_->snpset->contains_snp($_->name) } @{$snpset->snps}),
     'All SNPs and contained by parent');
}

sub named_snp : Test(1) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  ok((all { $snpset->named_snp($_->name)->name eq $_->name } @{$snpset->snps}),
     'All named SNPs');
}

sub references : Test(3) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  $data_object->add_avu('reference_name', 'ref1');
  $data_object->add_avu('reference_name', 'ref2');

  # References obtained from metadata, added automatically
  my $snpset1 = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  cmp_ok(scalar @{$snpset1->references}, '==', 2,
         'Has expected number of References');

  my @expected_names = qw(ref1 ref2);
  my @ref_names = map { $_->name } @{$snpset1->references};
  is_deeply(\@ref_names, \@expected_names,
            'Contains expected reference names') or diag explain \@ref_names;

  # No metadata, no references added automatically
  my $snpset2 = WTSI::NPG::Genotyping::SNPSet->new("$data_path/$data_file");
  cmp_ok(scalar @{$snpset2->references}, '==', 0,
         'Has no of References');
}

sub snp_names : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  my @snp_names = $snpset->snp_names;
  cmp_ok(scalar @snp_names, '==', 24, 'Contains expected number of SNP names');

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

sub snp_name_map : Test(24) {
    my $snpset = WTSI::NPG::Genotyping::SNPSet->new
        (file_name   => "$data_path/$data_file");
    my $snpset_renamed = WTSI::NPG::Genotyping::SNPSet->new
        (file_name   => "$data_path/$data_file_renamed");
    my $rename_map = $snpset->snp_name_map($snpset_renamed);
    foreach my $snp_name (keys %{$rename_map}) {
        my $expected;
        if ($snp_name eq 'rs11096957') {
            $expected = 'rs11096957_RENAMED';
        } else {
            $expected = $snp_name;
        }
        is($rename_map->{$snp_name}, $expected, "SNP name OK");
    }
}

sub union : Test(4) {

  my $irods = WTSI::NPG::iRODS->new;
  my $data_object_1 = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");
  my $snpset_1 = WTSI::NPG::Genotyping::SNPSet->new($data_object_1);
  my $data_object_2 = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file_2");
  my $snpset_2 = WTSI::NPG::Genotyping::SNPSet->new($data_object_2);
  my $union = $snpset_1->union([$snpset_2, ]);
  isa_ok($union, 'WTSI::NPG::Genotyping::SNPSet', "Union returns a SNPSet");
  is(scalar(@{$union->snps}), 30, "Correct size of union SNPSet");
  $union = $snpset_1->union([ ]);
  isa_ok($union, 'WTSI::NPG::Genotyping::SNPSet', "Union with empty list");
  is(scalar(@{$union->snps}), 24, "Correct size of union SNPSet");
}

sub write_snpset_file : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  my $multiplex = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  my $tmpdir = tempdir(CLEANUP => 1);
  my $test_file = "$tmpdir/$data_file";

  # 26, not 24 records because there are 2 records for each gender
  # marker
  cmp_ok($multiplex->write_snpset_data($test_file), '==', 26,
         "Number of records written to $test_file");

  my $expected_file = "$data_path/$data_file";
  ok(compare($test_file, $expected_file) == 0,
     "$test_file is identical to $expected_file");
}

1;
