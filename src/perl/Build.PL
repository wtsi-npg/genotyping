
use utf8;

use strict;
use warnings;
use lib qw(. lib);

use Module::Build;
use Build;

my $build = Build->new
  (module_name => 'WTSI::Genotyping',
   dist_abstract => 'WTSI genotyping data management tools',
   dist_version => 0.1.0,
   license  => 'gpl3',
   requires => {'Carp' => 0,
                'Test::More' => 0,
                'Test::Exception' => 0,
                'DBIx::Class' => 0,
                'Config::IniFiles' => 0},
   ini_files => {'etc/genders.ini' => 'etc/genders.ini',
                 'etc/methods.ini' => 'etc/methods.ini',
                 'etc/pipeline.ini' => 'etc/pipeline.ini',
                 'etc/relations.ini' => 'etc/relations.ini',
                 'etc/snpsets.ini' => 'etc/snpsets.ini',
                 'etc/states.ini' => 'etc/states.ini'},
   conf_files => {'etc/log4perl.conf' => 'etc/log4perl.conf'},
   sql_files => {'sql/pipeline_ddl.sql' => 'etc/pipeline_ddl.sql'},
   install_path => {'etc' => 'etc'}
  );

$build->add_build_element('conf');
$build->add_build_element('ini');
$build->add_build_element('sql');

$build->create_build_script;
