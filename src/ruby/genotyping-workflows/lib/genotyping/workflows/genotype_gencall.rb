#-- encoding: UTF-8
#
# Copyright (c) 2017 Genome Research Ltd. All rights reserved.
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

  class GenotypeGencall < Percolate::Workflow
    include Genotyping
    include Genotyping::Tasks::Metadata
    include Genotyping::Tasks::GenotypeCall
    include Genotyping::Tasks::Simtools
    include Genotyping::Tasks::Plink
    include Genotyping::Tasks::QualityControl


    description <<-DESC
Writes GenCall genotyping data in Plink binary format, and runs the
standard set of QC checks.

Requires a populated pipeline database.
    DESC

    usage <<-USAGE
GenotypeGencall args

Arguments:

- db_file <String>: The SQLite pipeline database file.
- run_name <String>: The name of a pipeline run defined in the pipeline
database.
- work_dir <String>: The working directory, an absolute path.
- other arguments (keys and values):

    - config: <path> of custom pipeline database .ini file. Optional.
    - manifest: <path> of the chip manifest file. Required.
    - plex_manifest: <Array> containing paths to one or more qc plex
      manifest files. If plex_manifest is supplied, vcf must also be given.
    - gender_method: <string> name of a gender determination method
      described in methods.ini. Optional, defaults to 'Inferred'
    - memory: <integer> number of Mb to request for jobs.
    - queue: <normal | long etc.> An LSF queue hint. Optional.
    - fam_dummy: <integer> Dummy value for missing paternal/maternal ID or
      phenotype in Plink .fam output. Must be equal to 0 or -9. Optional,
      defaults to -9.
    - vcf: <Array> containing paths to one or more VCF files for identity QC.
      If vcf is supplied, plex_manifest must also be given.

e.g.

 library: genotyping
 workflow: Genotyping::Workflows::GenotypeGencall
 arguments:
     - /work/my_project/my_analysis.db
     - sample_batch_1
     - /work/my_project/pipeline/
     - config: /work/my_project/pipeline/pipedb.ini
       queue: small
       manifest: /genotyping/manifests/Human670-QuadCustom_v1_A.bpm.csv
       vcf:
           - /work/my_project/qc_calls_foo.vcf
           - /work/my_project/qc_calls_bar.vcf
       plex_manifest:
           -/genotyping/manifests/qc_foo.tsv
           -/genotyping/manifests/qc_bar.tsv

Returns:

- boolean.
    USAGE

    #version '0.1.0'
    def run(dbfile, run_name, work_dir, args = {})
      defaults = {}
      args = intern_keys(defaults.merge(args))
      args = ensure_valid_args(args, :config, :manifest, :plex_manifest,
                               :queue, :memory, :select,
                               :fam_dummy, :gender_method, :vcf)

      async_defaults = {:memory => 1024}
      async = lsf_args(args, async_defaults, :memory, :queue, :select)

      manifest_raw = args.delete(:manifest)
      plex_manifest = args.delete(:plex_manifest) || Array.new()
      fam_dummy = args.delete(:fam_dummy) || -9
      gender_method = args.delete(:gender_method)
      config = args.delete(:config)
      vcf = args.delete(:vcf) || Array.new()

      args.delete(:memory)
      args.delete(:queue)
      args.delete(:select)

      if vcf.empty? and (not plex_manifest.empty?)
        raise ArgumentError, "Plex manifest must be accompanied by VCF"
      elsif (not vcf.empty?) and plex_manifest.empty?
        raise ArgumentError, "VCF must be accompanied by plex manifest"
      end

      ENV['PERL_INLINE_DIRECTORY'] = self.inline_dir

      work_dir = maybe_work_dir(work_dir)
      log_dir = File.join(work_dir, 'log')
      Dir.mkdir(log_dir) unless File.exist?(log_dir)
      args = {:work_dir => work_dir,
              :log_dir => log_dir}.merge(args)
      maybe_version_log(log_dir)

      run_name = run_name.to_s
      sjname = run_name + '.gencall.sample.json'
      njname = run_name + '.snp.json'
      cjname = run_name + '.chr.json'
      smname = run_name + '.sim'
      gciname = run_name + '.gencall.imajor.bed'
      gcsname = run_name + '.gencall.smajor.bed'

      sjson = sample_intensities(dbfile, run_name, sjname, args) 
      gcifile, * = gtc_to_bed(sjson, manifest_raw, gciname, args, async) 
      # Must use raw manifest for gtc_to_bed; manifest needs to be consistent with allele values encoded in GTC files. g2i requires an un-normalized manifest as input, carries out normalization itself, and writes normalized .bim files in output.
      gcsfile = transpose_bed(gcifile, gcsname, args, async)
      manifest_name = File.basename(manifest_raw, '.bpm.csv')
      manifest_name = manifest_name+'.normalized.bpm.csv'
      manifest = normalize_manifest(manifest_raw, manifest_name, args)
      njson, cjson = parse_manifest(manifest, njname, cjname, args)

      # generate .sim file to compute intensity metrics
      smargs = {:normalize => true }.merge(args)
      smfile = gtc_to_sim(sjson, manifest, smname, smargs, async)

      gcsfile = update_annotation(gcsfile, sjson, njson, fam_dummy,
                                 args, async)

      output = File.join(work_dir, 'gencall_qc')
      qcargs = {
        :run => run_name,
        :sim => smfile
      }.merge(args)
      if (not vcf.empty?) and (not plex_manifest.empty?)
        # use comma-separated lists of VCF/plex files in QC args
        qcargs = qcargs.merge({
          :vcf => vcf.join(","),
          :plex_manifest => plex_manifest.join(","),
          :sample_json => sjson
        })
      end

      gcquality = quality_control(dbfile, gcsfile, output, qcargs, async)

    end

  end

end
