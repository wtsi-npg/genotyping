
use utf8;

package WTSI::NPG::Startable;

use English;
use IPC::Run;
use Moose::Role;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Executable';

has 'started' => (is => 'rw', isa => 'Bool', default => 0);

has 'harness' => (is => 'rw', isa => 'IPC::Run');

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
