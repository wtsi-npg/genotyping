use utf8;

package WTSI::NPG::Runnable;

use Encode qw(decode);
use English;
use IPC::Run;
use Moose;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Executable';

=head2 run

  Example    : WTSI::NPG::Runnable->new(executable => 'ls',
                                        arguments  => ['/'])->run;
  Description: Run the executable with the supplied arguments and STDIN.
               STDIN, STDOUT and STDERR may be accessed via the methods of
               WTSI::NPG::Executable. Dies on no-zero exit of child. Returns
               $self.

  Returntype : WTSI::NPG::Runnable

=cut

sub run {
  my ($self) = @_;

  my @cmd = ($self->executable, @{$self->arguments});
  my $command = join q{, }, @cmd;
  $self->debug("Running '$command'");

  my $result;
  {
    local %ENV = %{$self->environment};

    $result = IPC::Run::run(\@cmd,
                            '<',  $self->stdin,
                            '>',  $self->stdout,
                            '2>', $self->stderr);
  }

  my $status = $CHILD_ERROR;
  if ($status) {
    my $signal = $status & 127;
    my $exit = $status >> 8;

    if ($signal) {
      $self->logconfess("Execution of '$command' died from signal: $signal");
    }
    else {
      $self->logconfess("Execution of '$command' failed with exit code: ",
                        "$exit and STDERR '", join(" ", $self->split_stderr),
                        "'");
    }
  }
  else {
    $self->debug("Execution of '$command' succeeded");
  }

  return $self;
}

=head2 split_stdout
  Example    : WTSI::NPG::Runnable->new(executable => 'ls',
                                        arguments  => ['/'])->run->split_stdout
  Description: If $self->stdout is a ScalarRef, dereference and split on the
               supplied delimiter (defaults to the input record separator).
               Raises an error if $self->stdout is not a ScalarRef.

  Returntype : Array[Str]

=cut

sub split_stdout {
  my ($self) = @_;

  ref $self->stdout eq 'SCALAR' or
    $self->logconfess('The stdout attribute was not a scalar reference');

  my $copy = decode('UTF-8', ${$self->stdout}, Encode::FB_CROAK);

  return split $INPUT_RECORD_SEPARATOR, $copy;
}

=head2 split_stderr
  Example    : WTSI::NPG::Runnable->new(executable => 'ls',
                                        arguments  => ['/'])->run->split_stderr
  Description: If $self->stderr is a ScalarRef, dereference and split on the
               supplied delimiter (defaults to the input record separator).
               Raises an error if $self->stderr is not a ScalarRef.

  Returntype : Array[Str]

=cut

sub split_stderr {
  my ($self) = @_;

  ref $self->stderr eq 'SCALAR' or
    $self->logconfess('The stderr attribute was not a scalar reference');

  my $copy = decode('UTF-8', ${$self->stderr}, Encode::FB_CROAK);

  return split $INPUT_RECORD_SEPARATOR, $copy;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Runnable

=head1 DESCRIPTION

An instance of this class enables an external program to be run (using
IPC::Run::run).

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
