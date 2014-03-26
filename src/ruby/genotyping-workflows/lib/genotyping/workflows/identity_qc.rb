#-- encoding: UTF-8
#
# Copyright (c) 2014 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

module Genotyping::Workflows

  class IdentityQC < Percolate::Workflow
    include Genotyping
    include Genotyping::Tasks::Metadata
    include Genotyping::Tasks::Plink
    include Genotyping::Tasks::QualityControl
    include Genotyping::Tasks::Simtools # needed for gtc_to_bed

    description <<-DESC
Convert GTC input files to Plink binary format and run the identity QC metric.
Requires a populated pipeline database containing QC plex calls (eg. Sequenom 
or Fluidigm)
    DESC

    usage <<-USAGE
IdentityQC args

Arguments:

- db_file (String): The SQLite pipeline database file.
- run_name (String): The name of a pipeline run defined in the pipeline database.
- work_dir (String): The working directory, an absolute path.
- other arguments (keys and values):

    - ini: <path> of custom pipeline database .ini file. Optional.
    - manifest: <path> of the bpm.csv manifest file. Required.
    - min_snps: <integer> minimum number of SNPs shared between the QC plex and
Plink dataset. Optional.
    - min_ident: <float> number between 0 and 1; minimum identity with the QC
plex for a smple to be marked as a pass. Optional.
    - memory: <integer> number of Mb to request for jobs.
    - queue: <normal | long etc.> An LSF queue hint. Optional, defaults to
    'normal'.

e.g.

 library: genotyping
 workflow: Genotyping::Workflows::IdentityQC
 arguments:
     - /work/my_project/my_analysis.db
     - sample_batch_1
     - /work/my_project/pipeline/
     - config: /work/my_project/pipeline/pipedb.ini
       queue: small
       manifest: /genotyping/manifests/Human670-QuadCustom_v1_A.bpm.csv

Returns:

- boolean.
    USAGE

    def run(dbfile, run_name, work_dir, args = {})

      defaults = {}
      args = intern_keys(defaults.merge(args))
      args = ensure_valid_args(args, :ini, :queue, :memory, :select,
                               :manifest, :min_snps, :min_ident, :db_file)
      async_defaults = {:memory => 1024}
      async = lsf_args(args, async_defaults, :memory, :queue, :select)
      
      manifest_raw = args.delete(:manifest)
      inipath = args.delete(:ini)
      dbfile = args.delete(:db_file)
      min_snps = args.delete(:min_snps) || 8
      min_ident = args.delete(:min_ident) || 0.9
      args.delete(:memory)
      args.delete(:queue)
      args.delete(:select)

      work_dir = maybe_work_dir(work_dir)
      log_dir = File.join(work_dir, 'log')
      Dir.mkdir(log_dir) unless File.exist?(log_dir)
      args = {:work_dir => work_dir,
              :log_dir => log_dir}.merge(args)
      maybe_version_log(log_dir)

      run_name = run_name.to_s;

      gcsjname = run_name + '.gencall.sample.json'
      gciname = run_name + '.gencall.imajor.bed'
      gcsname = run_name + '.gencall.smajor.bed'
      gcsjson = sample_intensities(dbfile, run_name, gcsjname, args) 
      gcifile, * = gtc_to_bed(gcsjson, manifest_raw, gciname, args, async) 
      gcsfile = transpose_bed(gcifile, gcsname, args, async)

      id_args = {:ini => inipath, :min_snps => min_snps, 
        :min_ident => min_ident}.merge(args)
      check_identity(dbfile, gcsfile, work_dir, id_args, async)

    end

  end # end class
end # end module
