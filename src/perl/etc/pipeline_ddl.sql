
--- A plate well address in various styles
DROP TABLE IF EXISTS address;
CREATE TABLE address (
id_address INTEGER PRIMARY KEY AUTOINCREMENT,
label1 TEXT NOT NULL UNIQUE,
label2 TEXT NOT NULL UNIQUE
);


--- A set of SNPs corresponding to a genotyping platform (Infinium or Sequenom)
DROP TABLE IF EXISTS snpset;
CREATE TABLE snpset (
id_snpset INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
version TEXT
);


--- A collection of samples from a supplier
DROP TABLE IF EXISTS dataset;
CREATE TABLE dataset (
id_dataset INTEGER PRIMARY KEY AUTOINCREMENT,
if_project text UNIQUE,
id_datasupplier INTEGER NOT NULL REFERENCES datasupplier(id_datasupplier),
id_snpset INTEGER NOT NULL REFERENCES snpset(id_snpset),
id_piperun INTEGER  NOT NULL REFERENCES piperun(id_piperun)
);


--- A supplier of samples
DROP TABLE IF EXISTS datasupplier;
CREATE TABLE datasupplier (
id_datasupplier INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
namespace TEXT NOT NULL UNIQUE
);


--- An organism gender
DROP TABLE IF EXISTS gender;
CREATE TABLE gender (
id_gender INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
code INTEGER NOT NULL UNIQUE
);


--- An analysis method
DROP TABLE IF EXISTS method;
CREATE TABLE method (
id_method INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
definition TEXT NOT NULL
);

--- Software metadata
DROP TABLE IF EXISTS pipemeta;
CREATE TABLE pipemeta (
schema_version TEXT NOT NULL,
pipeline_version TEXT NOT NULL
);


--- A pipeline run
DROP TABLE IF EXISTS piperun;
CREATE TABLE piperun (
id_piperun INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT UNIQUE NOT NULL,
start_time INTEGER,
finish_time INTEGER
);


--- A microtitre plate
DROP TABLE IF EXISTS plate;
CREATE TABLE plate (
id_plate INTEGER PRIMARY KEY AUTOINCREMENT,
ss_barcode text NOT NULL,
if_barcode text NOT NULL UNIQUE
);


--- A sample-sample relationship
DROP TABLE IF EXISTS related_sample;
CREATE TABLE related_sample (
id_sample_a INTEGER NOT NULL REFERENCES sample(id_sample),
id_sample_b INTEGER NOT NULL REFERENCES sample(id_sample),
id_relation INTEGER NOT NULL REFERENCES relation(id_relation),
PRIMARY KEY (id_sample_a, id_sample_b, id_relation)
);


--- A relation type
DROP TABLE IF EXISTS relation;
CREATE TABLE relation (
id_relation INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
definition TEXT NOT NULL
);


--- A result obtained for a sample by a method
DROP TABLE IF EXISTS result;
CREATE TABLE result (
id_sample INTEGER NOT NULL REFERENCES sample(id_sample),
id_method INTEGER REFERENCES method(id_method) NOT NULL,
value TEXT,
id_result INTEGER PRIMARY KEY AUTOINCREMENT
);


--- A DNA sample
DROP TABLE IF EXISTS sample;
CREATE TABLE sample (
id_sample INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
sanger_sample_id TEXT,
supplier_name TEXT,
cohort TEXT,
rowcol TEXT,
beadchip TEXT NOT NULL,
id_dataset INTEGER NOT NULL REFERENCES dataset(id_dataset),
include INTEGER NOT NULL
);


--- A sample-gender relationship
DROP TABLE IF EXISTS sample_gender;
CREATE TABLE sample_gender (
id_sample INTEGER NOT NULL REFERENCES sample(id_sample),
id_gender INTEGER NOT NULL REFERENCES gender(id_gender),
id_method INTEGER NOT NULL REFERENCES method(id_method),
PRIMARY KEY (id_sample, id_gender, id_method)
);


--- A known SNP defintion
DROP TABLE IF EXISTS snp;
CREATE TABLE snp (
id_snp INTEGER PRIMARY KEY,
name TEXT NOT NULL UNIQUE,
chromosome TEXT NOT NULL,
position INTEGER NOT NULL,
id_snpset INTEGER NOT NULL REFERENCES snpset(id_snpset)
);

CREATE UNIQUE INDEX uniq_snp_name ON snp(name);


--- A snp-result relationship
DROP TABLE IF EXISTS snp_result;
CREATE TABLE snp_result (
id_result INTEGER NOT NULL REFERENCES result(id_result),
id_snp INTEGER NOT NULL REFERENCES snp(id_snp),
value TEXT NOT NULL
);

CREATE INDEX idx_snp_result_result ON snp_result(id_result);
CREATE INDEX idx_snp_result_snp ON snp_result(id_snp);


--- A sample analysis state
DROP TABLE IF EXISTS state;
CREATE TABLE state (
id_state INTEGER PRIMARY KEY AUTOINCREMENT,
name TEXT NOT NULL UNIQUE,
definition TEXT NOT NULL
);


--- A sample-state relationship
DROP TABLE IF EXISTS sample_state;
CREATE TABLE sample_state (
id_sample INTEGER NOT NULL REFERENCES sample(id_sample),
id_state INTEGER NOT NULL REFERENCES state(id_state),
PRIMARY KEY (id_sample, id_state)
);


--- A microtitre plate well
DROP TABLE IF EXISTS well;
CREATE TABLE well (
id_well INTEGER PRIMARY KEY AUTOINCREMENT,
id_address INTEGER NOT NULL REFERENCES address(id_address),
id_plate integer NOT NULL REFERENCES plate(id_plate),
id_sample integer REFERENCES sample(id_sample)
);

CREATE INDEX well_idx_plate ON well(id_plate);
CREATE INDEX well_idx_sample ON well(id_sample);
CREATE UNIQUE INDEX uniq_well_plate_address ON well(id_address, id_plate);
