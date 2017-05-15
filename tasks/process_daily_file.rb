class ProcessDailyFile
  include Sidekiq::Worker
  sidekiq_options queue: :process_daily_file
  def perform(dataset, day)
    BaumgartnerDataset.new(dataset).extract_edges(day)
    BaumgartnerDataset.new(dataset).extract_self_loops(day)
    BaumgartnerDataset.new(dataset).extract_user_counts(day)
    BaumgartnerDataset.new(dataset).extract_user_starts(day)
  end
  
  def self.kickoff
    files = `ls /media/dgaff/backup/Code/reddit_random_walk/code/results/dataset_full/data/baumgartner_time_transitions/`.split("\n").select{|x| x.split("-").length == 3}
  end
end
#files.collect{|f| ProcessDailyFile.perform_async("full", f)}