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
      # TODO run QC on GenCall output (similar to Illuminus) & exclude failures
      defaults = {}
      args = intern_keys(defaults.merge(args))
      args = ensure_valid_args(args, :config, :manifest, :egt, :queue, :memory,
                               :select, :chunk_size, :zstart, :ztotal, :min_cr)

      async_defaults = {:memory => 1024}
      async = lsf_args(args, async_defaults, :memory, :queue, :select)

      manifest = args.delete(:manifest) 
      egt_file = args.delete(:egt) 
      chunk_size = args.delete(:chunk_size) || 4 # 20 for production
      gtconfig = args.delete(:config)
      zstart = args.delete(:zstart) || 6  # wider z range for production
      ztotal = args.delete(:ztotal) || 3
      min_cr = args.delete(:min_cr) || 0.9 # minimum gencall call rate

      args.delete(:memory)
      args.delete(:queue)
      args.delete(:select)

      work_dir = maybe_work_dir(work_dir)
      log_dir = File.join(work_dir, 'log')
      Dir.mkdir(log_dir) unless File.exist?(log_dir)
      args = {:work_dir => work_dir,
              :log_dir => log_dir}.merge(args)
      maybe_version_log(log_dir)

      gcsjname = run_name + '.gencall.sample.json'
      gciname = run_name + '.gencall.imajor.bed'
      gcsname = run_name + '.gencall.smajor.bed'
      sjname = run_name + '.sample.json'
      njname = run_name + '.snp.json'
      cjname = run_name + '.chr.json'
      zname = run_name + '.zcall.bed'

      njson, cjson = parse_manifest(manifest, njname, cjname, args)

      gcsjson = sample_intensities(dbfile, run_name, gcsjname, args) 
      gcifile, * = gtc_to_bed(gcsjson, manifest, gciname, args, async)
      gcsfile = transpose_bed(gcifile, gcsname, args, async)

      ## run gencall QC to apply gencall CR filter and find genders
      gcqcargs = {:run => run_name,
                  :post_filter_cr => min_cr}.merge(args)

      gcqcdir = File.join(work_dir, 'gencall_qc')
      gcquality = quality_control(dbfile, gcsfile, gcqcdir, gcqcargs, 
                                  async, true)

      sjson = nil
      if gcquality
        siargs = {:config => gtconfig}.merge(args)
        sjson = sample_intensities(dbfile, run_name, sjname, siargs)
      end
      num_samples = count_samples(sjson)

      result = nil
      if sjson
        result = prepare_thresholds(egt_file, zstart, ztotal, args, async)
      end
      tjson = nil
      tjson = result[0] if result
      evargs = {:samples => sjson,
                :start => 0,
                :end => num_samples,
                :size => chunk_size}.merge(args)
      evjson = evaluate_thresholds(tjson, sjson, manifest, egt_file, 
                                   evargs, async)
      metric_json = merge_evaluation(evjson, tjson, args, async)
      best_t = nil
      best_t = read_best_thresholds(metric_json) if metric_json

      zargs = {:start => 0,
               :end => num_samples,
               :size => chunk_size}.merge(args)
      zchunks_i, temp_dir = run_zcall_array(best_t, sjson, manifest, egt_file,
                                            zargs, async)
      zchunks_t = nil
      if zchunks_i
        transpose_args = args.clone
        transpose_args[:work_dir] = temp_dir
        zchunks_s = zchunks_i.each_with_index.collect do |bfile, i|
          bfile_s = File.join(temp_dir, ('zcall_smajor_part_%03d' % i)+'.bed')
        end
        zchunks_t = transpose_bed_array(zchunks_i, zchunks_s, 
                                        transpose_args, async)
      end
      

      zfile = update_annotation(merge_bed(zchunks_t, zname, args, async),
                                 sjson, njson, args, async)
      qcargs = {:run => run_name}.merge(args)
      zquality = quality_control(dbfile, zfile, 'zcall_qc', qcargs, async)

      [zfile, zquality] if [zfile, zquality].all?
    end

    :private
    def count_samples(sjson)
      JSON.parse(File.read(sjson)).size if sjson
    end

    def read_best_thresholds(mjson)
      metrics = JSON.parse(File.read(mjson)) if mjson
      return metrics['BEST_THRESHOLDS']
    end

  end # end of class

end # end of module
