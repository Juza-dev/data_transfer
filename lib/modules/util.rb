module MakeFolders
  def mkdir(root_path, name, date_string=nil)
      if date_string.nil?
          date = Time.new()
      else
          date = Date.strptime(date_string, '%Y%m%d')
      end
      path = "#{root_path}/#{name}/#{date.year}/#{date.month}/#{date.day}"
      p "mkdir -p #{path}"
      system("mkdir -p #{path}")
      return path
  end
end

module HandleDatabase
  def bulk_insert(class_name,array)
      target_class = Object.const_get(class_name)
      results = array.map{|r| target_class.new(r)}
      import_result = 0
      output = nil
      begin
          commit_time = Benchmark.realtime do
              output = target_class.import results, batch_size: 4000
          end
          Rails.logger.info("#{class_name} db commit: #{commit_time}s")
          import_result = output['ids'].count
      rescue => e
          Rails.logger.info(e.message)
      end
      return import_result
  end

  def bulk_upsert(class_name, array, keys, table_info)
      target_class = Object.const_get(table_info.camelize)
      results = array.map{|r| target_class.new(r)}
      headers = array[0].keys
      update_columns = headers - keys
      import_result = 0

      begin
          commit_time = Benchmark.realtime do
            constraint_name = keys
            output = target_class.import results, on_duplicate_key_update: {conflict_target: constraint_name, columns: update_columns}, batch_size: 4000
              Rails.logger.error("failed instances: #{output['failed_instances']}") if output['failed_instances'].count > 0
              import_result = output['ids'].count
          end
          #Rails.logger.info("#{class_name} db commit: #{commit_time}s")
          puts "table : #{class_name}  imported #{import_result} records"
      rescue StandardError => e
          #Rails.logger.error(e.message)
      end
      return import_result
  end
end