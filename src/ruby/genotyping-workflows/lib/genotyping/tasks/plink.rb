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

  PLINK = 'plink'
  UPDATE_ANNOTATION = 'update_plink_annotation.pl'

  # Returns true if the Plink executable is available.
  def plink_available?()
    system("which #{PLINK} >/dev/null 2>&1")
  end

  module Plink
    include Genotyping
    include Genotyping::Tasks

    # Runs Plink to merge an Array of BED format files into a single file.
    # file.
    #
    # Arguments:
    # - bed_files (Array): The BED file names.
    # - output (String): The output file name.
    # - args (Hash): Arguments for the operation.
    #
    # - async (Hash): Arguments for asynchronous management.
    #
    # Returns:
    # - The merged Plink BED file name
    def merge_bed(bed_files, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(bed_files, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        first_bed = bed_files.first
        rest_bed = bed_files.slice(1, bed_files.size - 1)

        merge_list = change_extname(output, '.parts')
        File.open(merge_list, 'w') do |fofn|
          rest_bed.sort.each { |bed| fofn.puts(plink_fileset(bed).join(' ')) }
        end

        first_name = File.join(File.dirname(first_bed),
                               File.basename(first_bed, '.bed'))
        expected = plink_fileset(output)

        cli_args = {:merge_list => merge_list,
                    :noweb => true,
                    :make_bed => true,
                    :bfile => first_name,
                    :out => File.basename(output, '.bed')}

        command = [PLINK, cli_arg_map(cli_args, :prefix => '--') { |key|
          key.gsub(/_/, '-')
        }].flatten.join(' ')

        margs = [bed_files, work_dir, output]
        task_id = task_identity(:merge_bed, *margs)
        log = File.join(log_dir, task_id + '.log')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected.first },
                   :async => async)
      end
    end

    def transpose_bed(bed_file, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(bed_file, output, work_dir)
        output = absolute_path?(output) ? output : absolute_path(output, work_dir)
        expected = plink_fileset(output)

        cli_args = {:noweb => true,
                    :make_bed => true,
                    :bfile => File.basename(bed_file, '.bed'),
                    :recode => true,
                    :transpose => true,
                    :out => File.basename(output, '.bed')}

        command = [PLINK, cli_arg_map(cli_args, :prefix => '--') { |key|
          key.gsub(/_/, '-')
        }].flatten.join(' ')

        margs = [bed_file, work_dir, output]
        task_id = task_identity(:transpose_bed, *margs)
        log = File.join(log_dir, task_id + '.log')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected.first },
                   :async => async)
      end
    end

    def transpose_bed_array(inputs, outputs, args = {}, async = {})
      # as for transpose_bed, but transpose multiple files in parallel
      # inputs/outputs are Plink .bed filenames
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(inputs, outputs, work_dir)
        if inputs.size != outputs.size
          raise 'Mismatched input/output list lengths'
        end
        targs = Array.new
        out_paths = Array.new
        commands = inputs.zip(outputs).collect do |input, output|
          cli_args = {
            :noweb => true,
            :make_bed => true,
            :bfile => File.basename(input, '.bed'),
            :recode => true,
            :transpose => true,
            :out => File.basename(output, '.bed')}
          targs.push(cli_args)
          out_paths.push(output)
          command = [PLINK, cli_arg_map(cli_args, :prefix => '--') { |key|
                       key.gsub(/_/, '-')
                     }].flatten
          command.join(' ')
        end
        margs_arrays = targs.collect { | args |
          [work_dir, args]
        }.each_with_index.collect { |elt, i| [i] + elt }
        task_id = task_identity(:transpose_bed_array, *margs_arrays)
        log = File.join(log_dir, task_id + '.%I.log')
        async_task_array(margs_arrays, commands, work_dir, log,
                         :post => lambda { 
                           ensure_files(out_paths, :error => false)
                         },
                         :result => lambda { |i| out_paths[i] },
                         :async => async)
      end
    end

    def update_annotation(bed_file, sample_json, snp_json, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(bed_file, sample_json, snp_json, work_dir)
        output = absolute_path?(bed_file) ? bed_file : absolute_path(bed_file, work_dir)
        expected = plink_fileset(output)

        cli_args = {:bed => bed_file,
                    :samples => sample_json,
                    :snps => snp_json}
        command = [UPDATE_ANNOTATION,
                   cli_arg_map(cli_args, :prefix => '--')].flatten.join(' ')

        margs = [bed_file, sample_json, snp_json, work_dir]
        task_id = task_identity(:update_annotation, *margs)
        log = File.join(log_dir, task_id + '.log')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected.first },
                   :async => async)
      end
    end

    :private
    def plink_fileset(bed)
      [bed, change_extname(bed, '.bim'), change_extname(bed, '.fam')]
    end

  end # module Plink
end # module Genotyping::Tasks
