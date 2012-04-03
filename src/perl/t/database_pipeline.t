
# Tests WTSI::Genotyping::Database::Pipeline

use strict;
use warnings;

use Test::More tests => 16;
use Test::Exception;

BEGIN { use_ok('WTSI::Genotyping::Schema'); }
require_ok('WTSI::Genotyping::Schema');

use WTSI::Genotyping::Database::Pipeline;

my $sqlite = 'sqlite3';
my $sql_path = './sql';
my $ini_path = './etc';

system("$sqlite t/genotyping.db < $sql_path/genotyping_ddl.sql") == 0
  or die "Failed to create test database: $?\n";

my $db = WTSI::Genotyping::Database::Pipeline->new
  (name => 'pipeline',
   inifile => "$ini_path/pipeline.ini");
ok($db);

dies_ok { $db->snpset->all }
  'Expected AUTOLOAD to fail when unconnected';

my $schema  = $db->connect(RaiseError => 1,
                           on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
ok($schema);

dies_ok { $db->should_not_autoload_this_method->all }
  'Expected AUTOLOAD to fail on invalid method';

$db->populate;
is(18, scalar $db->snpset->all);
is(3, scalar $db->method->all);
is(2, scalar $db->relation->all);
is(1, scalar $db->state->all);

my $supplier = $db->datasupplier->find_or_create({name => $ENV{'USER'},
                                                  namespace => 'wtsi'});

ok($supplier);

my $run = $db->piperun->find_or_create({name => 'test',
                                        start_time => time()});
ok($run);

my $project_base = 'test_project';
my $snpset = $db->snpset->find({name => 'HumanOmni2.5-8v1'});

my @datasets;
foreach my $i (1..3) {
  my $dataset =
    $run->add_to_datasets({if_project => sprintf("%s_%d", $project_base, $i),
                           datasupplier => $supplier,
                           snpset => $snpset});
  ok($dataset);
  push @datasets, $dataset;
}

my $sample_base = 'test_sample';
my $good = $db->state->find({name => 'Good'});
foreach my $i (1..1000) {
  my $sample = $datasets[0]->add_to_samples({name => sprintf("%s_%d",
                                                             $sample_base, $i),
                                             state => $good,
                                             beadchip => 'ABC123456',
                                             include => 1});
}

is(1000, scalar $datasets[0]->samples);
