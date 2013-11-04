package WTSI::NPG::iRODS::GroupAdmin;
use Moose;
use IPC::Run qw(start);

has '_in' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {my$t=''; return \$t},
);

has '_out' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {my$t=''; return \$t},
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
  while (1){ $h->pump; last if ${$self->_out}=~s/\r?\n\r?^groupadmin\>//sm; }
  return;
}

sub _push_pump_trim_split {
  my($self,$in)=@_;
  my $out_ref = $self->_out;
  ${$out_ref}=q();
  ${$self->_in} = $in;
  $self->_pump_until_prompt();
  ${$out_ref}=~s/\A\Q$in\E//smx;
  ${$out_ref}=~s/\r//smg;
  my@results=split /\r?\n\r?/, ${$out_ref};
  ${$out_ref}=q();
  return @results;
}

sub lg {
  my $self = shift;
  my $in = join q( ), q(lg), @_, qq(\n);
  my @results = $self->_push_pump_trim_split($in);
  if(@results and $results[0]=~/\AMembers of group/smx){
    shift @results;
  }
  return @results;
}

sub DEMOLISH {
  my ($self) = @_;
  ${$self->_out}=q();
  ${$self->_in}="quit\n";
  $self->_harness->finish;
}


1;
