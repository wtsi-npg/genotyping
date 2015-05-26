
use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't'
# directory is added because the Test::Class tests are there.
use lib qw(. t);

use WTSI::DNAP::Utilities::Build;
use BuildFluidigm;

my $build = BuildFluidigm->new
  (module_name     => 'WTSI::Genotyping',
   dist_author     => ['Iain Bancarz <ib5@sanger.ac.uk>',
                       'Keith James <kdj@sanger.ac.uk>'],
   dist_abstract   => 'WTSI genotyping data management tools',
   dist_version    => BuildFluidigm->report_version,
   license         => 'gpl',
   build_script    => 'BuildFluidigm',
   configure_requires => {
                          'Module::Build'   => 0.42
                         },
   test_requires      => {
                          'TAP::Harness'    => '>= 3.30',
                          'Test::Class'     => '>= 0.41',
                          'Test::More'      => '>= 0.98',
                          'Test::Exception' => '>= 0.32',
                         },
   requires          => {
                         'Config::IniFiles'              => '>= 2.8.6',
                         'DBI'                           => '>= 1.63',
                         'DBIx::Class'                   => '>= 0.082',
                         'Data::Dump'                    => '>= 1.22',
                         'DateTime'                      => '>= 1.18',
                         'List::AllUtils'                => '>= 0.9',
                         'Log::Log4perl'                 => '>= 1.46',
                         'Moose'                         => '>= 2.1',
                         'MooseX::Types'                 => '>= 0.45',
                         'Set::Scalar'                   => '>= 1.29',
                         'Text::CSV'                     => '>= 1.33',
                         'Try::Tiny'                     => '>= 0.22',
                         'URI'                           => '>= 1.67',
                         'WTSI::DNAP::Warehouse::Schema' => '>= 1.1',
                         'WTSI::NPG::iRODS'              => '>= 0.15.0'
                        },
   recommends        => {
                         'UUID' => '>= 0.24',
                        },
   test_files        => [
                         './t/call.t',
                         './t/fluidigm_assay_data_object.t',
                         './t/fluidigm_assay_result.t',
                         './t/fluidigm_assay_resultset.t',
                         './t/fluidigm_export_file.t',
                         './t/fluidigm_publisher.t',
                         './t/fluidigm_resultset.t',
                         './t/fluidigm_subscriber.t',
                         './t/gender_marker.t',
                         './t/publisher.t',
                         './t/simple_publisher.t',
                         './t/snp.t',
                         './t/snpset_publisher.t',
                         './t/utilities.t'
                        ]
  );

$build->create_build_script;
