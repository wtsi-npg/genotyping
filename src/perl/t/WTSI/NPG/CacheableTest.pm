use utf8;

{
  package WTSI::NPG::CacheableThing;

  use strict;
  use warnings;

  use Moose;

  with 'WTSI::NPG::Cacheable';

  my $meta = __PACKAGE__->meta;

  around 'get_scalar_1_arg' => sub {
    my ($orig, $self, $arg) = @_;

    my $cache = $self->get_method_cache($meta->get_method('get_scalar_1_arg'));

    return $self->get_with_cache($cache, $arg, $orig, $arg);
  };

  # 1 argument, use argument as key, return 1 scalar
  sub get_scalar_1_arg {
    my ($self, $arg) = @_;

    return $arg;
  }

  around 'get_scalar_n_arg' => sub {
    my ($orig, $self, @args) = @_;

    my $cache = $self->get_method_cache($meta->get_method('get_scalar_n_arg'));

    return $self->get_with_cache($cache, join('', @args), $orig, $args[0]);
  };

  # n arguments, concatenate to make a key, return 1 scalar
  sub get_scalar_n_arg {
    my ($self, @args) = @_;

    return join('', @args);
  }

  around 'get_ref_n_arg' => sub {
    my ($orig, $self, @args) = @_;

    my $cache = $self->get_method_cache($meta->get_method('get_ref_n_arg'));

    return $self->get_with_cache($cache, join('', @args), $orig, @args);
  };

  # n arguments, concatenate to make a key, return a reference
  sub get_ref_n_arg {
    my ($self, @args) = @_;

    return [@args];
  }
}

package WTSI::NPG::CacheableTest;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More tests => 6;
use Test::Exception;

use Log::Log4perl;

Log::Log4perl::init('./etc/log4perl_tests.conf');

sub get_scalar_1_arg : Test(2) {
  my $thing = WTSI::NPG::CacheableThing->new;

  foreach (0 .. 1) {
    is($thing->get_scalar_1_arg("x"), "x");
  }
}

sub get_scalar_n_arg : Test(2) {
  my $thing = WTSI::NPG::CacheableThing->new;

  foreach (0 .. 1) {
    is($thing->get_scalar_n_arg("x", "y", "z"), "x");
  }
}

sub get_ref_n_arg : Test(2) {
  my $thing = WTSI::NPG::CacheableThing->new;

  foreach (0 .. 1) {
    is_deeply($thing->get_ref_n_arg("x", "y", "z"), ["x", "y", "z"]);
  }
}
