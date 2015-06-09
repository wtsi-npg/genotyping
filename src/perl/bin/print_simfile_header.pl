#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# convenience script to look at the header and first few entries of a .sim file

use warnings;
use strict;
use WTSI::NPG::Genotyping::QC::SimFiles qw/printSimHeader/;

our $VERSION = '';

printSimHeader($ARGV[0]);

