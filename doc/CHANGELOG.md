
Change log for WTSI genotyping pipeline
=======================================

Latest version is hosted at: https://github.com/wtsi-npg/genotyping

Unreleased
----------

Added:
- Store QC plex calls in a VCF file instead of SQLite pipeline database
- QC check modified to read from VCF instead of database

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