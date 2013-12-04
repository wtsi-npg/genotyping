#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );
use WTSI::NPG::iRODS::GroupAdmin;
use npg_warehouse::Schema;

my $what_on_earth =<<'WOE';

Script to update WTSI iRODS systems with groups corresponding to Sequencescape studies.

Appropriate iRODS enviroment variables (e.g. irodsEnvFile) and files should be set and configured to allow access andupdate of the desired iRODS system.

The Sequencescape warehouse database is used to find the set of studies. iRODS groups are created for each study with names of the format ss_<study_id> when they do not already exist.

The iRODS zone is taken to have a preexisting "public" group which is used to identify all available users.

If a Sequencescape study has an entry for the "data_access_group" then the intersection of the members of the corresponding WTSI unix group and iRODS public group is used as the membership of the corresponding iRODS group.

If no data_access_group is set on the study, then if the study is associated with sequencing the members of the iRODS group will be set to the public group, else if the study is not assocaited with sequencing the iRODS group will be left empty (except for the iRODS groupadmin user).

Script runs to perform such updates when no arguments are given.

WOE


if(@ARGV){
  print {*STDERR} $what_on_earth;
  exit(0);
}

my $iga = WTSI::NPG::iRODS::GroupAdmin->new();
my@public=$iga->lg(q(public));
sub _uid_to_iRODSuid {
  my($u)=@_;
  return grep {/^\Q$u\E#/smx} @public;
}

my%ug2id; #cache
sub ug2id {
  my$g=shift||return;
  if(my$gha=$ug2id{$g}){return @{$gha};}
  $g=`getent group $g`;
  chomp $g;
  my@g = split q(,), (split q(:),$g)[-1]||q();
  $ug2id{$g}=\@g;
  return @g;
}

my $s=npg_warehouse::Schema->connect();
my$rs=$s->resultset(q(CurrentStudy));

my($group_count,$altered_count)= (0,0);
while (my$st=$rs->next){
  my$study_id=$st->internal_id;
  my$g=$st->data_access_group;
  my$is_seq=($st->npg_information->count||$st->npg_plex_information->count)>0;
  my@m=$g      ? map{ _uid_to_iRODSuid($_) } ug2id($g) :
       $is_seq ? @public :
                 ();
  $altered_count += $iga->set_group_membership("ss_$study_id",@m) ? 1 : 0;
  $group_count++;
}

if($altered_count){
  print {*STDERR} "$altered_count of $group_count iRODS groups created or membership altered (by ".($iga->_user).")\n";
}
