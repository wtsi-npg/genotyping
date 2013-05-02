#-- encoding: UTF-8
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

module Genotyping::Workflows

  class GenotypeZCall < Percolate::Workflow
    include Genotyping
    include Genotyping::Tasks::Metadata
    include Genotyping::Tasks::GenotypeCall
    include Genotyping::Tasks::GenoSNP
    include Genotyping::Tasks::Plink
    include Genotyping::Tasks::QualityControl
    include Genotyping::Tasks::ZCall

    description <<-DESC
    Takes as input GTC files with intensities and calls from default calling 
software, such as Illumina's GenCall. Applies the zCall method to re-call
any no-calls in the input, and outputs the results to a Plink BED format file.

Requires a populated pipeline database.
    DESC

    usage <<-USAGE
    GenotypeGenoSNP args

Arguments:

- db_file (String): The SQLite pipeline database file.
- run_name (String): The name of a pipeline run defined in the pipeline database.
- work_dir (String): The working directory, an absolute path.
- other arguments (keys and values):

    config: <path> of custom pipeline database .ini file. Optional.
    manifest: <path> of the chip manifest file. Required.
    egt:  <path> of the .EGT intensity cluster file. Required.
    chunk_size: <integer> number of samples to analyse in a single GenoSNP job.
    Optional, defaults to 20.
    zstart: <integer> start for range of candidate integer z scores. Optional.
    ztotal: <integer> total number of candidate integer z scores. Optional.
    memory: <integer> number of Mb to request for jobs.
    queue: <normal | long etc.> An LSF queue hint. Optional, defaults to
    'normal'.

e.g.

 library: genotyping
 workflow: Genotyping::Workflows::GenotypeGenoSNP
 arguments:
     - /work/my_project/my_analysis.db
     - sample_batch_1
     - /work/my_project/pipeline/
     - config: /work/my_project/pipeline/pipedb.ini
       queue: small
       manifest: /genotyping/manifests/Human670-QuadCustom_v1_A.bpm.csv
       egt: /genotyping/clusters/Human670-QuadCustom_v1.egt

Returns:

- boolean.
    USAGE

    def run(dbfile, run_name, work_dir, args = {})
      defaults = {}
      args = intern_keys(defaults.merge(args))
      args = ensure_valid_args(args, :config, :manifest, :egt, :queue, :memory,
                               :select, :chunk_size)

      async_defaults = {:memory => 1024}
      async = lsf_args(args, async_defaults, :memory, :queue, :select)

      manifest = args.delete(:manifest) 
      egt_file = args.delete(:egt) 
      chunk_size = args.delete(:chunk_size) || 4 # 20 for production
      gtconfig = args.delete(:config)
      zstart = args.delete(:zstart) || 6  # wider z range for production
      ztotal = args.delete(:ztotal) || 3

      args.delete(:memory)
      args.delete(:queue)
      args.delete(:select)

      work_dir = maybe_work_dir(work_dir)
      log_dir = File.join(work_dir, 'log')
      Dir.mkdir(log_dir) unless File.exist?(log_dir)
      args = {:work_dir => work_dir,
              :log_dir => log_dir}.merge(args)
      maybe_version_log(log_dir)

      sjname = run_name + '.sample.json'

      siargs = {:config => gtconfig}.merge(args)
      sjson = sample_intensities(dbfile, run_name, sjname, siargs)

      result = prepare_thresholds(egt_file, zstart, ztotal, args, async)
    end

  end # end of class

end # end of module
