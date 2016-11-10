
Change log for WTSI genotyping pipeline
=======================================

Latest version is hosted at: https://github.com/wtsi-npg/genotyping

Unreleased
----------

Added:
- Replace old identity check with new Bayesian version in "main" QC output
and plots

Release 1.13.1: 2016-07-28
--------------------------

Changed:
- Made install.sh more transparent and portable.
- Updated WTSI-DNAP-Utilities and perl-irods-wrap versions in install.sh.

Fixed:
- Default LSF queue for Ruby workflows


Release 1.13.2: 2016-11-10
--------------------------

Changed:
- Hotfix: Add cgp and ddd references to WTSI::NPG::Genotyping::QC::Identity


Release 1.13.1: 2016-07-28
--------------------------

Changed:
- Made install.sh more transparent and portable.
- Updated WTSI-DNAP-Utilities and perl-irods-wrap versions in install.sh.

Fixed:
- Default LSF queue for Ruby workflows


Release 1.13.0: 2016-06-20
--------------------------

Added:
- install.sh script to install pipeline and its Perl dependencies
- Documentation for Bayesian identity check

Changed:
- Modified ready_workflow.pl to better align with user SOP
- Use try/catch to handle unexpected errors in retrieving QC plex results
from iRODS
- Updated reference genome for Sequenom iRODS query
- Update perl-irods-wrap dependency to 2.4.0; removes unhelpful warning
messages to STDERR. This in turn requires baton version >= 0.16.4.

Removed:
- Script publish_infinium_file_list.pl; superseded by other publish scripts


Release 1.12.1: 2016-05-13
--------------------------

Fixed:
- Support repeat scans from Infinium database


Release 1.13.0: 2016-06-20
--------------------------

Added:
- install.sh script to install pipeline and its Perl dependencies
- Documentation for Bayesian identity check

Changed:
- Modified ready_workflow.pl to better align with user SOP
- Use try/catch to handle unexpected errors in retrieving QC plex results
from iRODS
- Updated reference genome for Sequenom iRODS query
- Update perl-irods-wrap dependency to 2.4.0; removes unhelpful warning
messages to STDERR. This in turn requires baton version >= 0.16.4.

Removed:
- Script publish_infinium_file_list.pl; superseded by other publish scripts


Release 1.12.1: 2016-05-13
--------------------------

Fixed:
- Support repeat scans from Infinium database


Release 1.12.0: 2016-04-07
--------------------------

Added:
- New ready_workflow.pl script to query iRODS for QC plex data, write VCF,
generate config YML, and set up an analysis directory
- Ruby workflows can read multiple VCF and plex manifest paths from
YML, and input them to the quality_control pipeline task
- Revised default pass threshold for Bayesian identity check

Removed:
- genotyping_yml.pl and tests; functionality replaced by ready_workflow.pl
- Annotation modules exporting constants for irods metadata; constants have
been relocated to Metadata.pm in wtsi-npg/perl-irods-wrap

Fixed:
- Refactored run_qc.pl for better handling of command-line arguments
- Updated for compatibility with latest dependency versions


Release 1.11.6: 2016-02-08
--------------------------

Fixed:
- Bug in WTSI::NPG::Expression::Publisher where it used a query against
the SequenceScape warehouse that was not specific enough to allow it
to proceed when a sample had been analysed more than once.


Release 1.11.5: 2015-12-15
--------------------------

Added:
- Support for 12-digit barcodes on Infinium gene expression arrays,
  retaining support for 10-digit barcodes.
- "Callset name" attribute for the Call class, allowing Call objects to
  be sorted into groups (eg. by genotyping platform)
- New Bayesian identity check:
  - Find Fluidigm/Sequenom results in iRODS and write as VCF
  - CSV and JSON output, including breakdown of QC results by callset name
  - Read QC calls from VCF instead of database
- Find genome reference path from iRODS metadata for VCF header; adds
  dependency on wtsi-npg/npg_tracking


Release 1.11.4: 2015-10-09
--------------------------

Fixed:
- Bug in publish_infinium_genotypes.pl which caused it to exit when it
  detected bad or missing data. It now detects these files and skips them.


Release 1.11.3: 2015-09-18
--------------------------

Added:
- QC plex manifest path now required in workflow YML input.

Fixed:
- Bug in run_qc.pl; failure to input plex manifest to WIP identity check.

Changed:
- bcftools version upgraded to 1.2


Release 1.11.2: 2015-08-24
--------------------------

Fixed:
- Bug (typo) in update_sequenom_qc_metadata.pl

Added:
- Tests to compile all scripts in ./bin
- Test dependency on Test::Compile

Removed:
- export_sequenom_genotypes.pl
- make_irods_study_group.pl
- populate_wtsi_irods_groups.pl
- print_simfile_header.pl
- set_irods_group_access.pl
- xydiff.pl


Release 1.11.1: 2015-08-12
--------------------------

Fixed:
- Bug in ready_infinium.pl which failed to support 12 digit Infinium
barcodes. Parsing of Infinium filenames is now more robust.


Release 1.11.0: 2015-07-23
--------------------------

Added:
- New script ready_qc_calls.pl to retrieve Fluidigm/Sequenom results
from iRODS and write as VCF (not yet in use for identity check in workflow)

Fixed:
- Bug (typo) in update_sequenom_metadata.pl
- Corrected evaluation of gender marker status

Modulefile:
- perl-irods-wrap/1.7.0
- baton/1.15.0


Release 1.10.0: 2015-07-15
--------------------------

Added:
- Support of 10, 11, or 12 digit Infinium barcodes in Types.pm
- Addition of MEGA_Consortium beadchip to snpsets.ini

Changed:
- Refactoring of VCF code to use WTSI::NPG result classes
- Increased compliance with Perlcritic
- Comma-separated years in copyright statements
- Add qscore attribute to Call class
- Use of Try::Tiny to replace eval
- General tidying up of code and spacing

Modulefile:
- Updated to use simtools version 2.2


Changelog started: 2015-07-15
-----------------------------

- Human-readable list of additions, changes, fixes, etc. to the WTSI
genotyping pipeline.
- Based on suggested format at http://keepachangelog.com/
