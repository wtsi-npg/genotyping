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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

module Genotyping::Workflows

  class GenotypeIlluminus < Percolate::Workflow
    include Genotyping
    include Genotyping::Tasks::Database
    include Genotyping::Tasks::GenotypeCall
    include Genotyping::Tasks::Illuminus
    include Genotyping::Tasks::Plink

    description <<-DESC
Collates the normalized intensity values and GenCall genotype calls for the
samples in one named pipeline run. The former are written into a single SIM
format file, the latter into a Plink BED format file. Calls genotypes using
Illuminus and writes them to an additional Plink BED format file.

Requires a populated pipeline database.
DESC

    usage <<-USAGE
GenotypeIlluminus args

Arguments:

- db_file (String): The SQLite pipeline database file.
- run_name (String): The name of a pipeline run defined in the pipeline database.
- work_dir (String): The working directory, an absolute path.
- other arguments (keys and values):

config: <path> of custom pipeline database .ini file. Optional.
manifest: <path> of the chip manifest file. Required.
memory: <integer> number of Mb to request for jobs.
queue: <normal | long etc.> An LSF queue hint. Optional, defaults to
'normal'.

e.g.

library: genotyping
workflow: Genotyping::Workflows::GenotypeIlluminus
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
      args = ensure_valid_args(args, :config, :manifest, :queue, :memory)

      async_defaults = {:memory => 1024,
                        :queue => :normal}
      async = lsf_args(args, async_defaults, :memory, :queue)

      manifest = args.delete(:manifest) # TODO: find manifest automatically
      args.delete(:memory)
      args.delete(:queue)

      work_dir = maybe_work_dir(work_dir)
      log_dir = File.join(work_dir, 'log')
      Dir.mkdir(log_dir) unless File.exist?(log_dir)
      args = {:work_dir => work_dir,
              :log_dir => log_dir}.merge(args)

      sjname = run_name + '.sample.json'
      njname = run_name + '.snp.json'
      cjname = run_name + '.chr.json'
      smname = run_name + '.illuminus.sim'
      gcname = run_name + '.gencall.bed'
      ilname = run_name + '.illuminus.bed'

      sjson = sample_intensities(dbfile, run_name, sjname, args)

      smargs = {:normalize => true,
                :chromosome_meta => cjname,
                :snp_meta => njname}.merge(args)

      smfile, cjson, njson = gtc_to_sim(sjson, manifest, smname, smargs, async)
      gcfile, * = gtc_to_bed(sjson, manifest, gcname, args, async)

      ilargs = {:size => 10000,
                :group_size => 50,
                :plink => true,
                :snps => njson}.merge(args)

      chunks = chromosome_bounds(cjson).collect do |cspec|
        chr = cspec["chromosome"]
        pargs = {:chromosome => chr,
                 :start => cspec["start"],
                 :end => cspec["end"]}

        call_from_sim_p(smfile, sjson, manifest, run_name + '.' + chr,
                        ilargs.merge(pargs), async)
      end

      ilfile = chunks.empty? ? nil : merge_bed(chunks.flatten, ilname, args, async)

      [gcfile, ilfile] if [gcfile, ilfile].all?
    end

    :private
    def chromosome_bounds(cjson)
      if cjson
        JSON.parse(File.read(cjson))
      else
        []
      end
    end
  end
end
