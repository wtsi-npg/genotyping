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

module Genotyping::Tasks

  GENOTYPE_CALL = 'genotype-call'

  def genotype_call_available?()
    system("which #{GENOTYPE_CALL} >/dev/null 2>&1")
  end

  module GenotypeCall
    include Genotyping::Tasks

    # Builds mock study data consisting of sample metadata (JSON), a SNP
    # manifest and GTC files.
    #
    # Arguments:
    # - study_name (String): The study name
    # - num_samples (Fixnum): The number of sample to generate.
    # - num_snps (Fixnum): The number of SNPs on the array.
    # - args (Hash): Arguments for the operation.
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An Array containing:
    #  - sample metadata (JSON) file name.
    #  - SNP manifest file name.
    #  - Array of GTC format file names.
    def mock_study(study_name, num_samples, num_snps, args = {}, async ={})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(study_name, num_samples, num_snps, work_dir)
        manifest = File.join(work_dir, "#{study_name}.bpm.csv")
        sample_json = File.join(work_dir, "#{study_name}.json")
        gtc_files = (0...num_samples).collect do |i|
          File.join(work_dir, sprintf("%s_%04d.gtc", study_name, i))
        end

        cli_args = {:study_name => study_name,
                    :num_samples => num_samples,
                    :num_snps => num_snps,
                    :manifest => manifest}
        margs = [cli_args, work_dir]
        task_id = task_identity(:mock_study, *margs)
        log = File.join(log_dir, task_id + '.log')

        command =[GENOTYPE_CALL, 'mock-study',
                  cli_arg_map(cli_args,
                              :prefix => '--') { |key|
                    key.gsub(/_/, '-') }].flatten.join(' ')
        expected = [sample_json, manifest, gtc_files].flatten

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { [sample_json, manifest, gtc_files] },
                   :async => async)
      end
    end

    # Collates intensity data from multiple GTC format files into a single SIM
    # format file with JSON annotation of chromosome boundaries.
    #
    # Arguments:
    # - input (String): A JSON file specifying sample URIs and GTC file paths.
    # - manifest (String): The BeadPool manifest file name.
    # - output (String): The SIM file name.
    # - args (Hash): Arguments for the operation.
    #
    #   :chromosome (String): Limit the operation to SNPs on one chromosome, as
    #   named in the BeadPool manifest.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An Array containing
    #   - The SIM file path.
    #   - The JSON chromosome annotation path.
    def gtc_to_sim(input, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(input, manifest, output, work_dir)
        output = absolute_path(output, work_dir) unless absolute_path?(output)
        expected = [output]

        cli_args = {:chromosome => args[:chromosome],
                    :input => input,
                    :manifest => manifest,
                    :output => output}

        if args.has_key?(:chromosome_meta)
          chr_json = args[:chromosome_meta]
          chr_json = absolute_path(chr_json, work_dir) unless absolute_path?(chr_json)
          cli_args[:chromosome_meta] = chr_json
          expected << chr_json
        end

        if args.has_key?(:snp_meta)
          snp_json = args[:snp_meta]
          snp_json = absolute_path(snp_json, work_dir) unless absolute_path?(snp_json)
          cli_args[:snp_meta] = snp_json
          expected << snp_json
        end

        margs = [cli_args, input, work_dir]
        task_id = task_identity(:gtc_to_sim, *margs)
        log = File.join(log_dir, task_id + '.log')

        command = [GENOTYPE_CALL, 'gtc-to-sim',
                   cli_arg_map(cli_args, :prefix => '--') { |key|
                     key.gsub(/_/, '-') }].flatten.join(' ')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected },
                   :async => async)
      end
    end

    # Collates GenCall genotype call data from multiple GTC format files into a
    # single Plink BED format file.
    #
    # Arguments:
    # - input (String): A JSON file specifying sample URIs and GTC file paths.
    # - manifest (String): The BeadPool manifest file name.
    # - output (String): The BED file name.
    # - args (Hash): Arguments for the operation.
    #
    #   :chromosome (String): Limit the operation to SNPs on one chromosome, as
    #   named in the BeadPool manifest.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - An Array containing
    #   - The BED file path.
    #   - The JSON chromosome annotation path.
    def gtc_to_bed(input, manifest, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(input, manifest, output, work_dir)
        output = absolute_path(output, work_dir) unless absolute_path?(output)
        expected = [output]

        cli_args = {:chromosome => args[:chromosome],
                    :input => input,
                    :manifest => manifest,
                    :output => output}

        if args.has_key?(:chromosome_meta)
          chr_json = args[:chromosome_meta]
          chr_json = absolute_path(chr_json, work_dir) unless absolute_path?(chr_json)
          cli_args[:chromosome_meta] = chr_json
          expected << chr_json
        end

        margs = [cli_args, input, work_dir]
        task_id = task_identity(:gtc_to_bed, *margs)
        log = File.join(log_dir, task_id + '.log')

        command = [GENOTYPE_CALL, 'gtc-to-bed',
                   cli_arg_map(cli_args, :prefix => '--') { |key|
                     key.gsub(/_/, '-') }].flatten.join(' ')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected },
                   :async => async)
      end
    end

  end # module GenotypeCall
end # module Genotyping::Tasks
