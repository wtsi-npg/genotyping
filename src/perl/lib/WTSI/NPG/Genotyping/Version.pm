
package WTSI::NPG::Genotyping::Version;

use strict;
use warnings;
use Carp;

use vars qw/@ISA @EXPORT_OK/;
use Exporter;
@ISA = qw/Exporter/;
@EXPORT_OK = qw/version_text write_version_log/;

our $VERSION = '';

our $YEARS = '2014, 2015, 2016';

sub version_text {
    my $text = "WTSI Genotyping Pipeline version $VERSION\n".
        "Pipeline software copyright (C) $YEARS Genome Research Ltd.\n".
        "All rights reserved.\n";
    return $text;
}

sub write_version_log {
    # record version in given directory
    my $logdir = shift;
    if (!(-e $logdir && -d $logdir)) { 
        croak "Invalid log directory \"$logdir\"";
    }
    my $logpath =  $logdir."/version.log";
    open my $log, ">", $logpath || 
        croak "Cannot open version log \"$logpath\"";
    print $log version_text();
    close $log || croak "Cannot close version log \"$logpath\"";
    return 1;
}

1;
