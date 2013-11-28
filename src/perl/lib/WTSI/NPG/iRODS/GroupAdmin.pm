package WTSI::NPG::iRODS::GroupAdmin;
use Moose;
use IPC::Run qw(start run);
use File::Which qw(which);
use Cwd qw(abs_path);
use List::MoreUtils qw(any none);
use Readonly;
use Carp;

=head1 NAME

WTSI::NPG::iRODS::GroupAdmin

=head1 VERSION

1.0.0

=head1 SYNOPSIS

  use WTSI::NPG::iRODS::GroupAdmin;
  my $iga = WTSI::NPG::iRODS::GroupAdmin->new();
  print join",",$iga->lg;
  print join",",$iga->lg(q(public));

=head1 DESCRIPTION

A class for running iRODS group admin related commands for creating groups and altering their membership

=head1 SUBROUTINES/METHODS

=cut

Readonly::Scalar our $VERSION => q(1.0.0);
Readonly::Scalar our $IGROUPADMIN => q(igroupadmin);
Readonly::Scalar our $IENV => q(ienv);

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
                 my $in_ref = $self->_in;
                 ${$in_ref} = "\n"; #prevent initial hang - fetch the chicken...
                 my $out_ref = $self->_out;
                 # workaround Run::IPC caching : https://rt.cpan.org/Public/Bug/Display.html?id=57393
                 my $cmd = which $IGROUPADMIN;
                 if (not $cmd) { croak qq(Command '$IGROUPADMIN' not found)}
                 my $h = start [abs_path $cmd], q(<pty<), $in_ref, q(>pty>), $out_ref;
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
  if ( ${$out_ref}=~m/^ERROR:[^\n]+\z/smx ) {
    croak 'igroupadmin error: '.${$out_ref};
  }
  my@results=split /\n/smx, ${$out_ref};
  ${$out_ref}=q();
  return @results;
}

sub __croak_on_bad_group_name {
  my($group)=@_;
  if( (not defined $group ) or $group eq q()){
      croak q(empty string group name does not make sense to iRODs);
  }elsif ($group =~ /"/smx){
      croak qq(Cannot cope with group names containing double quotes '"' : $group);
  }
  return;
}

=head2 lg

List groups if not argument given, or list members of the group given as argument.

=cut

sub lg {
  my($self,$group)=@_;
  my $in = q(lg);
  if(defined $group){
    __croak_on_bad_group_name($group);
    $in .= qq( "$group");
  }
  $in .= qq(\n);
  my @results = $self->_push_pump_trim_split($in);
  if(defined $group){
    my $leadingtext = shift @results;
    if( @results and not $leadingtext=~/\AMembers\ of\ group/smx) {
      croak qq(unexpected text: \"$leadingtext\");
    }
  }
  if (@results==1 and $results[0]=~/\ANo\ rows\ found/smx ){
    shift @results;
    if (@results==0 and defined $group and not grep {$group eq $_} $self->lg){
      croak qq(group "$group" does not exist);
    }
  }
  return @results;
}


has '_user' => (
  'is' => 'ro',
  'isa' => 'Str',
  'builder' => '_build__user',
);
sub _build__user {
  my $out = q();
  my $cmd = which $IENV;
  if (not $cmd) { croak qq(Command '$IENV' not found)}
  run [abs_path $cmd], q(>), \$out;
  if (my ($u) = $out =~ m/irodsUserName=(\S+)/smx and my ($z) = $out =~ m/irodsZone=(\S+)/smx) {
    return "$u#$z";
  } else {
    croak 'Could not obtain user and zone from ienv: '.$out;
  }
}

sub _op_g_u {
  my($self,$op,$group,$user)=@_;
  __croak_on_bad_group_name($group);
  if( (not defined $user ) or $user eq q()){
      croak q(empty string username does not make sense to iRODs);
  }elsif ($user =~ /"/smx){
      croak qq(Cannot cope with username containing double quotes '"' : $group);
  }
  my $in = qq($op "$group" "$user"\n);
  $self->_push_pump_trim_split($in);
  return;
}

sub _ensure_existence_of_group {
  my($self,$group)=@_;
  __croak_on_bad_group_name($group);
  if ( any {$group eq $_} $self->lg){ return;}
  $self->_push_pump_trim_split(qq(mkgroup "$group"\n));
  return;
}

=head2 set_group_membership

Given a group and list of members will ensure that the group exists and contains exactly this admin user and the members (adding or removing as appropriate)

=cut

sub set_group_membership {
  my($self,$group,@members)=@_;
  $self->_ensure_existence_of_group($group);
  my @orig_members = $self->lg($group);
  if (@orig_members){
    if(none {$_ eq $self->_user} @orig_members) {carp "group $group does not contain user ".($self->_user).': authorization failure likely';}
  }else{
    $self->_op_g_u('atg',$group, $self->_user); #add this user to empty group (first) so admin rights to operate on it are retained
    push @orig_members, $self->_user;
  }
  my%members = map{$_=>1}@members,$self->_user;
  @orig_members = grep{ not delete $members{$_}} @orig_members; #make list to delete from orginal members if not in new list, leaves member to add in hash
  foreach my $m (@orig_members){ $self->_op_g_u('rfg',$group,$m); }
  @members = keys %members;
  foreach my $m (@members) { $self->_op_g_u('atg',$group,$m); }
  return;
}

sub BUILD {
  my ($self) = @_;
  $self->_harness; #ensure we start igroupadmin at object creation (and so with expected environment: environment variables used by igroupadmin)
  return;
}

sub DEMOLISH {
  my ($self) = @_;
  if($self->_out and $self->_in){
    ${$self->_out}=q();
    ${$self->_in}="quit\n";
    $self->_harness->finish;
  }
  return;
}


1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

Will honour iRODS related environment at time of object creation

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item Moose

=item IPC::Run

=item File::Which

=item Cwd

=item List::MoreUtils

=item Readonly

=item Carp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

  In ad-hoc testing different numbers of results for the same query have been seen - but v rarely and not reproducibly!

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

