
use utf8;

package WTSI::NPG::Startable;

use English;
use IPC::Run;
use Moose::Role;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Executable';

has 'started' =>
  (is      => 'rw',
   isa     => 'Bool',
   default => 0);

has 'harness' =>
  (is  => 'rw',
   isa => 'IPC::Run');

sub BUILD {
  my ($self) = @_;

  my @cmd = ($self->executable, @{$self->arguments});
  $self->harness(IPC::Run::harness(\@cmd,
                                   $self->stdin,
                                   $self->stdout,
                                   $self->stderr));
}

sub start {
  my ($self) = @_;

  if ($self->started) {
    $self->logwarn("Lister has started; cannot restart it");
    return $self;
  }

  my @cmd = ($self->executable, @{$self->arguments});
  my $command = join q{ }, @cmd;
  $self->debug("Starting '$command'");

  {
    local %ENV = %{$self->environment};
    IPC::Run::start($self->harness);
  }

  $self->started(1);

  return $self;
}

sub stop {
  my ($self) = @_;

  unless ($self->started) {
    $self->logwarn("Lister has not started; cannot stop it");
    return $self;
  }

  my @cmd = ($self->executable, @{$self->arguments});
  my $command = join q{ }, @cmd;
  $self->debug("Stopping '$command'");

  my $harness = $self->harness;
  eval { $harness->finish };

  if ($EVAL_ERROR) {
    my $err = $EVAL_ERROR;
    $harness->kill_kill;
    $self->logconfess($err);
  }
  $self->started(0);

  return $self;
}

sub DEMOLISH {
  my ($self) = @_;

  $self->stop;

  return;
}

no Moose;

1;


__END__

=head1 NAME


=head1 SYNOPSIS


=head1 DESCRIPTION


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
