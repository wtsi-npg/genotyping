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

module Genotyping
  VERSION = '0.8.4'
  YEAR = '2013'
  VERSION_LOG_NAME = 'version.log'

  def maybe_version_log(log_dir)
    unless File.exist?(File.join(log_dir, VERSION_LOG_NAME))
      write_version_log(log_dir)
    end
  end

  def version_text()
    text = "WTSI Genotyping Pipeline version "+VERSION+"\n"+
      "Pipeline software copyright (c) "+YEAR+" Genome Research Ltd.\n"+
      "All rights reserved.\n"
  end

  def write_version_log(log_dir)
    log = File.open(File.join(log_dir, VERSION_LOG_NAME), 'w')
    log.write(version_text())
    log.close
  end

end

