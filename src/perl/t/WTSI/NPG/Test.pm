package WTSI::NPG::Test;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More;

# Run full tests (requiring a test iRODS server) only if TEST_AUTHOR
# is true. If full tests are run, require that irodsEnvFile be set.
sub runtests {
  my ($self) = @_;

  my $env_file = $ENV{'WTSI_NPG_iRODS_Test_irodsEnvFile'} || q{};
  if (not $env_file) {
    if ($ENV{TEST_AUTHOR}) {
      die 'Environment variable WTSI_NPG_iRODS_Test_irodsEnvFile was not set';
    }
    else {
      $self->SKIP_CLASS('TEST_AUTHOR environment variable is false');
    }
  }

  my %env_copy = %ENV;
  # Ensure that the iRODS connection details are a nonsense value if
  # they are not set explicitly via WTSI_NPG_iRODS_Test_irodsEnvFile
  $env_copy{'irodsEnvFile'} = $env_file || 'DUMMY_VALUE';

  {
    local %ENV = %env_copy;
    return $self->SUPER::runtests;
  }
}

# If any test methods fail to complete, count all their remaining
# tests as failures.
sub fail_if_returned_early {
  return 1;
}

1;
