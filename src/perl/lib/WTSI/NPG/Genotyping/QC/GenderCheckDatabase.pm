
# Author:  Iain Bancarz, ib5@sanger.ac.uk
# January 2013

#
# Copyright (c) 2013 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

# Module containing database functions for gender check
# Accesses internal pipeline DB; not needed for standalone gender cehck

use warnings;
use strict;
use Carp;
use Exporter;
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/getDatabaseObject/;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/readDatabaseGenders updateDatabase/;

sub readDatabaseGenders {
    # read inferred genders from database -- use for testing database update
    # return hash of genders indexed by sample URI (not sample name)
    my $dbfile = shift;
    my $method = shift;
    $method ||= 'Inferred';
    my $db = getDatabaseObject($dbfile);
    my @samples = $db->sample->all;
    my %genders;
    $db->in_transaction(sub {
	foreach my $sample (@samples) {
	    my $sample_uri = $sample->uri;
	    my $gender = $db->gender->find
		({'sample.id_sample' => $sample->id_sample,
		  'method.name' => $method},
		 {join => {'sample_genders' => ['method', 'sample']}},
		 {prefetch =>  {'sample_genders' => ['method', 'sample']} });
	    $genders{$sample_uri} = $gender->code;
	}
			});
    $db->disconnect();
    return %genders;
}

sub updateDatabase {
    # update pipeline database with inferred genders
    # assume that sample names are given in URI format
    my ($uriRef, $gendersRef, $dbfile, $runName) = @_;
    my @uris = @$uriRef;
    my @genders = @$gendersRef;
    my %genders;
    for (my $i=0;$i<@uris;$i++) {
        $genders{$uris[$i]} = $genders[$i];
    }
    my $db = getDatabaseObject($dbfile);
    my $inferred = $db->method->find({name => 'Inferred'});
    my $run = $db->piperun->find({name => $runName});
    unless ($run) {
        croak "Run '$runName' does not exist. Valid runs are: [" .
            join(", ", map { $_->name } $db->piperun->all) . "]\n";
    }
    # transaction to update sample genders
    my @datasets = $run->datasets->all;
    foreach my $ds (@datasets) {
	my @samples = $ds->samples->all;
	$db->in_transaction(sub {
	    foreach my $sample (@samples) {
            # if sample already has an inferred gender, do not update!
            my $inferredGenders = 
                $sample->sample_genders->find({method => $inferred});
            if ($inferredGenders) { next; }
            my $sample_uri = $sample->uri;
            my $genderCode = $genders{$sample_uri};
            if (!defined($genderCode)) { $genderCode = 3; } # not available
            my $gender;
            if ($genderCode==1) { 
                $gender = $db->gender->find({name => 'Male'}); 
            } elsif ($genderCode==2) { 
                $gender = $db->gender->find({name => 'Female'}); 
            } elsif ($genderCode==0) { 
                $gender = $db->gender->find({name => 'Unknown'}); 
            } else { 
                $gender = $db->gender->find({name => 'Not Available'}); 
            }
            $sample->add_to_genders($gender, {method => $inferred});
	    }
                        });
    }
    $db->disconnect();
    return 1;
}
