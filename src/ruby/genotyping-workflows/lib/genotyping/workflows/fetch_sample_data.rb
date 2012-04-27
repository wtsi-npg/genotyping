#-- encoding: UTF-8
#
# Copyright (c) 2012 Genome Research Ltd. All rights reserved.
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

  class FetchSampleData < Percolate::Workflow
    include Genotyping
    include Genotyping::Tasks::Database
    include Genotyping::Tasks::GenotypeCall

    description <<-DESC
    Collates the normalized intensity values and GenCall genotype calls for the
samples in one named pipeline run. The former are written into a single SIM
format file, the latter into a Plink BED format file. Requires a populated
pipeline database.
    DESC

    usage <<-USAGE
FetchSampleData args

Arguments:

- db_file (String): The SQLite pipeline database file.
- run_name (String): The name of a pipeline run defined in the pipeline database.
- work_dir (String): The working directory, an absolute path.
- other arguments (keys and values):

    config: <path> of custom pipeline database .ini file. Optional.
    manifest: <path> of the chip manifest file. Required.
    memory: <integer> number of Mb to request.
    queue: <normal | long etc.> An LSF queue hint. Optional, defaults to
    'normal'.

e.g.

 library: genotyping
 workflow: Genotyping::Workflows::FetchSampleData
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

    version '0.1.0'

    def run(dbfile, run_name, work_dir, args = {})
      defaults = {}
      args = intern_keys(defaults.merge(args))

      args = ensure_valid_args(args, :config, :manifest, :queue)
      args[:work_dir] = maybe_work_dir(work_dir)
      manifest = args.delete(:manifest) # TODO: find manifest automatically

      async_defaults = {:memory => 500,
                        :queue => :normal}
      async = lsf_args(args, async_defaults, :memory, :queue)

      sample_json = run_name + '.json'
      sim_file = run_name + '.sim'
      bed_file = run_name + '.bed'

      sample_json = sample_intensities(dbfile, run_name, sample_json, args)
      [gtc_to_sim(sample_json, manifest, sim_file, args, async),
       gtc_to_bed(sample_json, manifest, bed_file, args, async)].flatten.all?
    end
  end

end
