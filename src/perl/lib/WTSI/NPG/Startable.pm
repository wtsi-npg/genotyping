
use utf8;

package WTSI::NPG::Startable;

use English;
use IPC::Run;
use Moose::Role;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Executable';

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

  my @cmd = ($self->executable, @{$self->arguments});
  my $command = join q{ }, @cmd;
  $self->debug("Starting '$command'");

  IPC::Run::start($self->harness);

  return $self;
}

sub stop {
  my ($self) = @_;

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

  return $self;
}

sub DEMOLISH {
  my ($self) = @_;

  $self->finish;

  return;
}

no Moose;

1;
