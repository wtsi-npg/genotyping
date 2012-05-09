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
    # - An Array of 3 filenames, being the Plink BED and corresponding BIM and
    #  FAM files.
    def merge_bed(bed_files, output, args = {}, async = {})
      args, work_dir, log_dir = process_task_args(args)

      if args_available?(bed_files, output, work_dir)
        output = absolute_path(output, work_dir) unless absolute_path?(output)
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
        task_id = task_identity(:bed_merge, *margs)
        log = File.join(log_dir, task_id + '.log')

        async_task(margs, command, work_dir, log,
                   :post => lambda { ensure_files(expected, :error => false) },
                   :result => lambda { expected },
                   :async => async)
      end
    end

    :private
    def plink_fileset(bed)
      [bed, change_extname(bed, '.bim'), change_extname(bed, '.fam')]
    end

  end # module Plink
end # module Genotyping::Tasks
