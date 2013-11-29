use utf8;

package WTSI::NPG::Runnable;

use English;
use IPC::Run;
use Moose;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Executable';

=head2 run

  Example    : WTSI::NPG::Runnable->new(executable => 'ls',
                                        arguments  => ['/'])->run
  Description: Run the executable with the supplied arguments and STDIN
               string. Capture STDOUT and STDERR as strings and return an
               array of STDOUT split on the output record separator.
  Returntype : Array Str

=cut

sub run {
  my ($self) = @_;

  my @cmd = ($self->executable, @{$self->arguments});
  my $command = join q{ }, @cmd;
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
      $self->logconfess("Execution of '$command' failed with exit code: $exit");
    }
  }
  else {
    $self->debug("Execution of '$command' succeeded");
  }

  my @stdout_records = split $INPUT_RECORD_SEPARATOR, ${$self->stdout};

  return @stdout_records;
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
