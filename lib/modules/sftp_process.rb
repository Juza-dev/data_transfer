require 'date'

class SftpProcess
    attr_reader :sftp

    def initialize(config)
        p config
        @sftp = Net::SFTP.start(config['url'], config['username'], :password => config['password'], :port => config['port'])
    end

    def upload_file(filename, local_path, remote_path)

        other_path = "#{remote_path}/#{filename}"
        mine_path = "#{local_path}/#{filename}"
        result = sftp.upload!(mine_path, other_path)
    end

    def download_file(filename, remote_path, local_path)

        other_path = "#{remote_path}/#{filename}"
        mine_path = "#{local_path}/#{filename}"
        sftp.download!(other_path, mine_path)
        return mine_path
    end

    def list_of_files(file_type, remote_path, date_str=nil)
        filename_arr = []
        date_str = Date.today.strftime("%Y%m%d") if date_str.nil?
        sftp.dir.foreach(remote_path) do |entry|
            if entry.name.include?(file_type) && entry.name.include?(date_str)
                filename_arr << entry.name
            end
        end
        return filename_arr
    end

    def list_of_files_ca(file_type, remote_path)
        filename_arr = []
        sftp.dir.foreach(remote_path) do |entry|
            if entry.name.include?(file_type)
                filename_arr << entry.name
            end
        end
        return filename_arr
    end

    def list_of_export(export)
        filename_arr = []
        files = Dir.glob("#{export}/*")
        files.each do |fn|
            name = File.split(fn)[1]
            filename_arr << name
        end
        return filename_arr
    end
end