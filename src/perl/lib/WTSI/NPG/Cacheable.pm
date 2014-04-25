use utf8;

package WTSI::NPG::Cacheable;

use Cache::MemoryCache;
use Moose::Role;

with 'WTSI::NPG::Loggable';

has 'method_caches' =>
  (is       => 'ro',
   isa      => 'HashRef',
   required => 1,
   default  => sub { return {} });

=head2 get_method_cache

  Arg [1]    : Class::MOP::Method method.
  Arg [2]    : HashRef initargs for Cache::MemoryCache (optional).

  Example    : my $method = $meta->get_method('find_well_status');
               my $cache = $self->get_method_cache($method,
                                                   {default_expires_in => 60});

  Description: Return an existing cache or make a new one if noe exists.

  Returntype : Cache::MemoryCache

=cut

sub get_method_cache {
  my ($self, $method, $cache_initargs) = @_;

  $method or $self->logconfess('A method argument is required');

  my $method_name = $method->fully_qualified_name;
  my $cache = $self->method_caches->{$method_name};

  if ($cache) {
    $self->debug("Found a cache for method '$method_name'")
  }
  else {
    $self->debug("Making a new cache for method '$method_name'");

    my $initargs = {};

    if ($cache_initargs) {
      ref $cache_initargs eq 'HASH' or
        $self->logconfess('Cache initargs must be a HashRef');


      foreach my $initarg (keys %$cache_initargs) {
        $initargs->{$initarg} = $cache_initargs->{$initarg};
      }
    }

    $initargs->{namespace} = $method_name;

    $cache = Cache::MemoryCache->new($initargs);
    $self->method_caches->{$method_name} = $cache;
  }

  return $cache;
}

=head2 get_with_cache

  Arg [1]    : Cache::MemoryCache cache.
  Arg [2]    : Scalar key.
  Arg [2]    : Class::MOP::Method fallback method.
  Arg [n]    : Arguments for fallback method call.

  Example    : $self->get_with_cache($cache, $key, $method, 'x', 'y');

  Description: Tries to get a value from the cache using a key. If the value
               is not in the cache, calls the fallback method with the
               arguments and installs the result in the cache before returning
               the cached value.

  Returntype : Any

=cut

sub get_with_cache {
  my ($self, $cache, $key, $method, @args) = @_;

  $cache or $self->logconfess('A cache argument is required');
  $method or $self->logconfess('A method argument is required');

  defined $key or $self->logconfess('A defined key argument is required');

  my $namespace = $cache->get_namespace;
  my $value = $cache->get($key);

  if (defined $value) {
    $self->debug("Cache hit for $namespace : $key -> $value");
  }
  else {
    $value = $self->$method(@args);

    my $value_str = defined $value ? 'undef' : $value;
    $self->debug("Cache miss for $namespace : $key -> $value_str");
    $cache->set($key, $value);
  }

  return $value;
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Cacheable

=head1 DESCRIPTION

A Role providing methods for the management of per-method caches of
return values. The caches are created usig Cache::MemoryCache. The
namespace of each cache is the fully qualified method name.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
