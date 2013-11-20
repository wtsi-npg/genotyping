
use utf8;

package WTSI::NPG::Executable;

use IPC::Run;;
use Moose::Role;

has 'stdin'  => (is => 'ro', isa => 'ScalarRef',
                 default => sub { my $x = ''; return \$x; });
has 'stdout' => (is => 'ro', isa => 'ScalarRef',
                 default => sub { my $x = ''; return \$x; });
has 'stderr' => (is => 'ro', isa => 'ScalarRef',
                 default => sub { my $x = ''; return \$x; });

has 'environment' => (is => 'ro', isa => 'HashRef', lazy => 1,
                      default => sub { \%ENV });
has 'executable' => (is => 'ro', isa => 'Str', required => 1);
has 'arguments'  => (is => 'ro', isa => 'ArrayRef', lazy => 1,
                     default => sub { [] });



no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Executable

=head1 DESCRIPTION

A Role providing attributes to represent a single run of an external
program by some method of IPC.

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
