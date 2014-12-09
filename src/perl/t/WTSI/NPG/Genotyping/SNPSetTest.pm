
use utf8;

package WTSI::NPG::Genotyping::SNPSetTest;

use strict;
use warnings;

use File::Compare;
use File::Temp qw(tempdir);
use List::AllUtils qw(all);

use base qw(Test::Class);
use File::Spec;
use Test::More tests => 16;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::Genotyping::SNPSet'); }

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

my $data_path = './t/snpset';
my $data_file = 'qc.csv';

my $irods_tmp_coll;

my $pid = $$;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods_tmp_coll = $irods->add_collection("SNPSetTest.$pid");
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $irods_path = "$irods_tmp_coll/snpset/$data_file";

  $irods->add_object_avu($irods_path, 'snpset', 'qc');
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new;
  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::Genotyping::SNPSet');
}

sub constructor : Test(5) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

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
}

sub snps : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  cmp_ok(scalar @{$snpset->snps}, '==', 26,
         'Contains expected number of SNPs');

  ok((all { $_->snpset->contains_snp($_->name) } @{$snpset->snps}),
     'All SNPs and contained by parent');
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

sub write_snpset_file : Test(2) {
  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "$irods_tmp_coll/snpset/$data_file");

  my $multiplex = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  my $tmpdir = tempdir(CLEANUP => 1);
  my $test_file = "$tmpdir/$data_file";

  cmp_ok($multiplex->write_snpset_data($test_file), '==', 26,
         "Number of records written to $test_file");

  my $expected_file = "$data_path/$data_file";
  ok(compare($test_file, $expected_file) == 0,
     "$test_file is identical to $expected_file");
}

1;
