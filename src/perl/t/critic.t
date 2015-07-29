use strict;
use warnings;

use Test::More;

eval {
  require Test::Perl::Critic;
};

if ($@) {
  plan skip_all => 'Test::Perl::Critic not installed';
} else {
  Test::Perl::Critic->import(
                             -severity => 1,
                             -profile => 't/perlcriticrc',
                             -verbose => "%m at %f line %l, policy %p\n");
  all_critic_ok();
}

1;
