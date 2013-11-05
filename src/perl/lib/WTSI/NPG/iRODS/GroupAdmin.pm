package WTSI::NPG::iRODS::GroupAdmin;
use Moose;
use IPC::Run qw(start);

=head1 NAME

npg_tracking::Schema

=head1 VERSION

$LastChangedRevision: 16389 $

=head1 SYNOPSIS

  use WTSI::NPG::iRODS::GroupAdmin;
  my $iga = WTSI::NPG::iRODS::GroupAdmin->new();
  print join",",$iga->lg;
  print join",",$iga->lg(q(public));

=head1 DESCRIPTION

A class for running iRODS group admin related commands for creating groups and altering their membership

=head1 SUBROUTINES/METHODS

=cut

has '_in' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {my$t=q(); return \$t},
);

has '_out' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {my$t=q(); return \$t},
);


has '_harness' => (
  'is' => 'ro',
  'builder' => '_build__harness',
  'lazy' => 1, #lazy as we need _in and _out to be instantiated before creating the harness
);

sub _build__harness {
                 my ($self) = @_;
                 my $out_ref = $self->_out;
                 my $h = start [qw(igroupadmin)], q(<pty<), $self->_in, q(>pty>), $out_ref;
                 $self->_pump_until_prompt($h);
                 ${$out_ref}=q();
                 return $h;
}

sub _pump_until_prompt {
  my($self,$h)=@_;
  $h ||= $self->_harness;
  while (1){ $h->pump; last if ${$self->_out}=~s/\r?\n^groupadmin\>//smx; }
  return;
}

sub _push_pump_trim_split {
  my($self,$in)=@_;
  my $out_ref = $self->_out;
  ${$out_ref}=q();
  ${$self->_in} = $in;
  $self->_pump_until_prompt();
  ${$out_ref}=~s/\r//smxg; #igroupadmin inserts CR before LF - remove all
  ${$out_ref}=~s/\A\Q$in\E//smx;
  my@results=split /\n/smx, ${$out_ref};
  ${$out_ref}=q();
  return @results;
}

sub lg {
  my($self,$group)=@_;
  my $in = q(lg);
  if($group){ $in .= " $group";}
  $in .= qq(\n);
  my @results = $self->_push_pump_trim_split($in);
  if(@results and $results[0]=~/\AMembers\ of\ group/smx){
    shift @results;
  }
  return @results;
}

sub DEMOLISH {
  my ($self) = @_;
  ${$self->_out}=q();
  ${$self->_in}="quit\n";
  $self->_harness->finish;
  return;
}


1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

Will honour iRODS related environment at time of IPC::Run harness creation

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item Moose

=item IPC::Run

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

  In ad-hoc testing differnt numbers of results for the same query have been seen - but v rarely and not reproducibly!

  The harness is created using a lazy build method therefore the environment of the underlying igroupadmin command for an object will be that of the program when the first method is called

=head2 AUTHOR

David K. Jackson <david.jackson@sanger.ac.uk>

=head2 LICENSE AND COPYRIGHT

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
                                                                                                                                                                                                                           1317,1        Bot

