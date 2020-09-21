namespace :refresh do
  desc 'mview_refresh'
  task :run => :environment do
      Rails.logger.info "mview_refresh started"
      views = ["all_auc","all_bit","rfm__auc__base","rfm__auc__base_new"]
      views.each do |view|
          Rails.logger.info "refreshing #{view} ..."
          sql = "REFRESH MATERIALIZED VIEW #{view}"
          result = ActiveRecord::Base.connection.exec_query(sql)
      end
      Rails.logger.info "mview_refresh completed"
  end
end