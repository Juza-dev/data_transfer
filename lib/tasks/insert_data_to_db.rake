require 'modules/sftp_process'
require 'modules/util'
include MakeFolders
include HandleDatabase
require "csv"


namespace :insert_data_to_db do
  desc 'insert db ftp data to aws db'
  task :run, [:date, :end_date, :type] => :environment do |t, args|
      args.with_defaults(date: Date.today.strftime("%Y%m%d"))
      args.with_defaults(end_date: Date.today.strftime("%Y%m%d"))
      if args[:type].nil?
          args.with_defaults(type: ['carinfo', 'aucinfo', 'bitinfo','bitinfo2','bitinfo3','memberinfo'])
          type_array = args[:type]
      else
          type_array = [args[:type]]
      end

      count = 0
      constraints = {'carinfo'=> [:inum], 'aucinfo'=> [:seq], 'bitinfo'=> [:seq], 'bitinfo2'=> [:seq], 'bitinfo3'=> [:seq], 'memberinfo'=> [:seq_id]}
      loop_count = Date.parse(args[:end_date]) - Date.parse(args[:date]) + 1
      target_date = Date.parse(args[:date]).strftime("%Y%m%d")

      while loop_count >= 1 do
        path_init(target_date)
        download_file_from_ftp(target_date)
        upsert_data_to_db(target_date, type_array,constraints)
        puts "#{target_date}のデータ取り込み処理終了"
        loop_count -= 1
        count += 1
        target_date = (Date.parse(args[:date]) + count).strftime("%Y%m%d")
      end
  end

end

private

def path_init(date)
  @mc_config = YAML.load_file('config/settings/ftp_info.yml')["f_customer"]["mc"]
  @root_path = Rails.root.join("tmp")
  @local_path = MakeFolders.mkdir(@root_path,'download', date)
  @log_path = MakeFolders.mkdir(@root_path,'log', date)
  @remote_path = '/Import/BI'
end

def download_file_from_ftp(date)
  mc_server = SftpProcess.new(@mc_config)

  list_of_tsv_file = mc_server.list_of_files('tsv', @remote_path, date)

  @local_file_paths = []
  list_of_tsv_file.each do |file|
      @local_file_paths << mc_server.download_file(file, @remote_path, @local_path)
  end
end

def upsert_data_to_db(target_date, types,constraints)
  types.each do |type|
    puts "#{type}の取り込み開始"
    csv_path = csv_path_check(target_date, type)
    csv_read_delete_upsert(csv_path,type,constraints)
  end
end

def csv_read_delete_upsert(csv_path,type,constraints)
  if csv_path == []
    puts "#{type}の取り込み対象ファイルは無し。#{type}の取り込みをスキップします。"
  end
  csv_path.each do |file|
    puts "取り込み対象ファイル#{file}"
    db_head = YAML.load_file("config/settings/data_info.yml")[type]["db_head"]
    csv_hash = csv_read(file,db_head)
    table_info = table_info(type)
    csv_upsert(csv_hash,type,constraints,table_info)
  end
end

def csv_conversion(file)
  base_file = File.open(file, "rt")
  buffer = base_file.read()
  buffer.gsub!(/(\r\n|\n|\r)/, "\r\n")
  new_file = File.open("#{file}.tmp", "w")
  new_file.write(buffer)
  new_file.close()
end

def csv_read(file,db_head)
  csv_conversion(file)
  file = "#{file}.tmp"
  csv_hash = []
  CSV.foreach(file, encoding: 'BOM|UTF-8', headers: db_head, col_sep: "\t", liberal_parsing: true).with_index do |row, index|
    row.each do |edit_row|
      edit_row[1].gsub!(/(\r\n|\n|\r)/ , "\r\n")
    end
    if row[0].present?
      csv_hash << row.to_hash.symbolize_keys
    end
  end
  File.delete(file)
  csv_hash
end

def csv_upsert(csv_hash,type,constraints, table_info)
  csv_length = csv_hash.count
  import_result = HandleDatabase.bulk_upsert(type, csv_hash, constraints[type], table_info)
  Rails.logger.info "imported #{import_result} records"
  Rails.logger.error "#{ csv_length - import_result } record failed" if csv_length - import_result > 0
end

def csv_path_check(target_date, type)
  csv_path = []
  @local_file_paths.each do |file|
      if file.include?("#{type}_bi_#{target_date}")
          csv_path << file
      end
  end
  csv_path
end

def table_info(type)
  if type == 'carinfo'
    return 'car_info'
  elsif type == 'aucinfo'
    return 'auc_info'
  elsif type == 'bitinfo'
    return 'bit_info'
  elsif type == 'bitinfo2'
    return 'bit_info2'
  elsif type == 'bitinfo3'
    return 'bit_info3'
  elsif type == 'memberinfo'
    return 'member_info'
  end
end