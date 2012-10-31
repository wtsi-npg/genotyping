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

  class GenotypeGenoSNP < Percolate::Workflow
    include Genotyping
    include Genotyping::Tasks::Database
    include Genotyping::Tasks::GenotypeCall
    include Genotyping::Tasks::GenoSNP
    include Genotyping::Tasks::Plink
    include Genotyping::Tasks::QualityControl

    description <<-DESC
    Collates the raw intensity values for the samples in one named pipeline run
to a single SIM format file. Calls genotypes using GenoSNP and writes them to a
Plink BED format file.

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
    chunk_size: <integer> number of samples to analyse in a single GenoSNP job.
    Optional, defaults to 20.
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

Returns:

- boolean.
    USAGE

    version '0.1.0'

    def run(dbfile, run_name, work_dir, args = {})
      defaults = {}
      args = intern_keys(defaults.merge(args))
      args = ensure_valid_args(args, :config, :manifest, :queue, :memory,
                               :chunk_size)

      async_defaults = {:memory => 1024}
      async = lsf_args(args, async_defaults, :memory, :queue)

      manifest = args.delete(:manifest) # TODO: find manifest automatically
      chunk_size = args.delete(:chunk_size) || 20
      args.delete(:memory)
      args.delete(:queue)

      work_dir = maybe_work_dir(work_dir)
      log_dir = File.join(work_dir, 'log')
      Dir.mkdir(log_dir) unless File.exist?(log_dir)
      args = {:work_dir => work_dir,
              :log_dir => log_dir}.merge(args)

      sjname = run_name + '.sample.json'
      njname = run_name + '.snp.json'
      smname = run_name + '.genosnp.sim'
      gsname = run_name + '.genosnp.bed'

      sjson = sample_intensities(dbfile, run_name, sjname, args)
      num_samples = count_samples(sjson)

      smargs = {:normalize => false,
                :snp_meta => njname}.merge(args)

      smfile, njson = gtc_to_sim(sjson, manifest, smname, smargs, async)
      gsargs = {:samples => sjson,
                :start => 0,
                :end => num_samples,
                :size => chunk_size,
                :group_size => 50,
                :plink => true,
                :debug => true}.merge(args)

      gschunks = call_from_sim_p(smfile, njson, manifest, run_name + '.' + chunk_size.to_s,
                                 gsargs, async)
      gschunks = gschunks.flatten if gschunks
      gsfile = update_annotation(merge_bed(gschunks, gsname, args, async),
                                 sjson, njson, args, async)

      qcargs = {:run => run_name}.merge(args)
      gsquality = quality_control(dbfile, gsfile, 'genosnp_qc', qcargs)

      [gsfile, gsquality] if [gsfile, gsquality].all?
    end

    :private
    def count_samples(sjson)
      JSON.parse(File.read(sjson)).size if sjson
    end

  end
end
