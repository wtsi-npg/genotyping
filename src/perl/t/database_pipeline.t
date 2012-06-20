
# Tests WTSI::Genotyping::Database::Pipeline

use utf8;

use strict;
use warnings;

use Test::More tests => 26;
use Test::Exception;

use Data::Dumper;

BEGIN { use_ok('WTSI::Genotyping::Schema'); }
require_ok('WTSI::Genotyping::Schema');

use WTSI::Genotyping::Database::Pipeline;

my $ini_path = './etc';
my $dbfile = 't/pipeline.db';
unlink($dbfile);

my $db = WTSI::Genotyping::Database::Pipeline->new
  (name => 'pipeline',
   inifile => "$ini_path/pipeline.ini",
   dbfile => $dbfile);
ok($db, 'A pipeline Database');
ok(-e $dbfile);
ok($db->disconnect);
undef $db;

$db = WTSI::Genotyping::Database::Pipeline->new
  (name => 'pipeline',
   inifile => "$ini_path/pipeline.ini",
   dbfile => $dbfile);
ok($db, 'A database file reopened.');

ok(WTSI::Genotyping::Database::Pipeline->new
   (name => 'pipeline',
    inifile => "$ini_path/pipeline.ini",
    dbfile => $dbfile,
    overwrite => 1), 'A database file overwrite');

dies_ok { $db->snpset->all }
  'Expected AUTOLOAD to fail when unconnected';

my $schema = $db->connect(RaiseError => 1,
                          on_connect_do => 'PRAGMA foreign_keys = ON')->schema;
ok($schema, 'A database Schema');

dies_ok { $db->should_not_autoload_this_method->all }
  'Expected AUTOLOAD to fail on invalid method';

$db->populate;
is(18, $db->snpset->count, 'The snpset dictionary');
is(3, $db->method->count, 'The method dictionary');
is(2, $db->relation->count, 'The relation dictionary');
is(1, $db->state->count, 'The state dictionary');

my $supplier = $db->datasupplier->find_or_create({name => $ENV{'USER'},
                                                  namespace => 'wtsi'});
ok($supplier, 'A supplier inserted');

my $run = $db->piperun->find_or_create({name => 'test',
                                        start_time => time()});
ok($run, 'A run inserted');

my $project_base = 'test_project';
my $snpset = $db->snpset->find({name => 'HumanOmni25-8v1'});
ok($snpset, 'A snpset found');

my @datasets;
foreach my $i (1..3) {
  my $dataset =
    $run->add_to_datasets({if_project => sprintf("%s_%d", $project_base, $i),
                           datasupplier => $supplier,
                           snpset => $snpset});
  ok($dataset, 'A dataset inserted');
  push @datasets, $dataset;
}

my $sample_base = 'test_sample';
my $good = $db->state->find({name => 'Good'});
ok($good, 'A state found');

my $bad = $db->state->find({name => 'Bad'});

$db->in_transaction(sub {
                      foreach my $i (1..1000) {
                        my $sample = $datasets[0]->add_to_samples
                          ({name => sprintf("%s_%d", $sample_base, $i),
                            beadchip => 'ABC123456',
                            include => 1});
                        $sample->add_to_states($good);
                      }
                    });

my @samples = $datasets[0]->samples;
is(1000, scalar @samples, 'Expected samples found');
my @states = $samples[0]->states;
is(1, scalar @states);
is('Good', $states[0]->name);

dies_ok {
  $db->in_transaction(sub {
                        foreach my $i (1001..2000) {
                          my $sample = $datasets[0]->add_to_samples
                            ({name => sprintf("%s_%d", $sample_base, $i),
                              beadchip => 'ABC123456',
                              include => 1});
                          $sample->add_to_states($good);

                          if ($i == 1900) {
                            die "Test error at $i\n";
                          }
                        }
                      });
} 'Expected transaction to fail';

is(1000, scalar $datasets[0]->samples, 'Successful rollback');
