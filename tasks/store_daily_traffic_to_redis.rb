class StoreDailyTrafficToRedis
  include Sidekiq::Worker
  sidekiq_options queue: :daily_edges_redis
  def perform(dataset_tag, file_set, strftime_str, cumulative_post_cutoff, percentile, only_higher)
    file_set.each do |files|
      counts = {}
      files.each do |file|
        CSV.foreach(BaumgartnerDataset.new(dataset_tag.split("")[1..-1].join("")).time_transitions+"/"+file) do |row|
          if only_higher
            if $redis.get("global_user_counts_#{row.last}").to_i >= cumulative_post_cutoff
              counts[row[1]] ||= {}
              counts[row[1]][row.first] ||= 0
              counts[row[1]][row.first] += 1
            end
          else
            if $redis.get("global_user_counts_#{row.last}").to_i <= cumulative_post_cutoff
              counts[row[1]] ||= {}
              counts[row[1]][row.first] ||= 0
              counts[row[1]][row.first] += 1
            end
          end
        end
      end
      current_i = 0
      counts.each do |target_subreddit, source_subreddits|
        source_subreddits.each_slice(500) do |source_subreddit_slice|
          file = nil
          if strftime_str == "%Y-%m-%d"
            file = files.first
          elsif strftime_str == "%Y-%m"
            file = files.first.split("-")[0..1].join("-")
          elsif strftime_str == "%Y"
            file = files.first.split("-").first
          end
          RedisStorer.new.hash_set("#{only_higher == true ? "higher" : "lower"}#{dataset_tag}_#{file}_#{strftime_str}_#{percentile}:#{current_i}",[target_subreddit, source_subreddit_slice].flatten.join(","))
          current_i += 1
        end
      end
    end
  end
end
