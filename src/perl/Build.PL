
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
   R_files => {
       '../r/bin/beanplotCR.R' => 'bin/beanplotCR.R',
       '../r/bin/beanplotHet.R' => 'bin/beanplotHet.R',
       '../r/bin/beanplotXYdiff.R' => 'bin/beanplotXYdiff.R',
       '../r/bin/boxplotCR.R' => 'bin/boxplotCR.R',
       '../r/bin/boxplotHet.R' => 'bin/boxplotHet.R',
       '../r/bin/boxplotXYdiff.R' => 'bin/boxplotXYdiff.R',
       '../r/bin/check_xhet_gender.R' => 'bin/check_xhet_gender.R',
       '../r/bin/heatmapCrHetDensity.R' => 'bin/heatmapCrHetDensity.R',
       '../r/bin/plates.R' => 'bin/plates.R',
       '../r/bin/plotCombinedFails.R' => 'bin/plotCombinedFails.R',
       '../r/bin/plotCrHetDensity.R' => 'bin/plotCrHetDensity.R',
       '../r/bin/plotCrPlate.R' => 'bin/plotCrPlate.R',
       '../r/bin/plotHetPlate.R' => 'bin/plotHetPlate.R',
       '../r/bin/plotIndividualFails.R' => 'bin/plotIndividualFails.R',
       '../r/bin/plotXYdiffDensity.R' => 'bin/plotXYdiffDensity.R',
       '../r/bin/plotXYdiffPlate.R' => 'bin/plotXYdiffPlate.R',
       '../r/bin/sample_delta_xy.R' => 'bin/sample_delta_xy.R',
       '../r/bin/scatterPlotFails.R' => 'bin/scatterPlotFails.R'},
   install_path => {'etc' => 'etc'}
  );

$build->add_build_element('conf');
$build->add_build_element('ini');
$build->add_build_element('sql');
$build->add_build_element('R');

$build->create_build_script;
