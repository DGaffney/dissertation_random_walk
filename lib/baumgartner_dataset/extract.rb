module BaumgartnerExtract
  def accumulate_edges
    daily_edges_final = daily_edges_file.gsub("daily_edges", "daily_edges_final")
    `mkdir #{daily_edges_final}`
    all_edges = {}
    `ls #{daily_self_loops}`.split("\n").each do |file|
      daily_edges = []
      CSV.open(daily_edges_file+"/"+file).each do |row|
        if row.first != row.last
          if all_edges[row.join("_")].nil?
            all_edges[row.join("_")] = true
            daily_edges << row
          end
        end
      end
      csv = CSV.open(daily_edges_final+"/"+file, "w")
      daily_edges.collect{|e| csv << e};false
      csv.close
    end
  end

  def accumulate_self_loops
    self_loops_final = daily_self_loops.gsub("self_loops", "self_loops_final")
    `mkdir #{self_loops_final}`
    all_edges = {}
    `ls #{daily_self_loops}`.split("\n").each do |file|
      `cp #{daily_self_loops}/#{file} #{self_loops_final}/#{file}`
    end
  end

  def accumulate_user_counts
    user_counts_final = daily_user_counts.gsub("user_counts", "user_counts_final")
    `mkdir #{user_counts_final}`
    all_edges = {}
    `ls #{daily_self_loops}`.split("\n").each do |file|
      `cp #{daily_user_counts}/#{file} #{user_counts_final}/#{file}`
    end
  end

  def accumulate_user_starts
    user_starts_final = daily_user_starts.gsub("user_starts", "user_starts_final")
    observed_users = {}
    `mkdir #{user_starts_final}`
    all_edges = {}
    `ls #{daily_self_loops}`.split("\n").each do |file|
      users_today = {}
      CSV.open("#{daily_user_starts}/#{file}").each do |row|
        if observed_users[row.first].nil?
          observed_users[row.first] = row.last
          users_today[row.first] = row.last
        end
      end
      csv = CSV.open("#{user_starts_final}/#{file}", "w")
      users_today.collect{|k,v| csv << [k,v]}
      csv.close
    end
  end

  def accumulate_extracted_data
    accumulate_edges
    accumulate_self_loops
    accumulate_user_counts
    accumulate_user_starts
  end

  def daily_edges_file
    "#{project_folder}/data/daily_edges/"
  end

  def daily_self_loops
    "#{project_folder}/data/self_loops/"
  end

  def daily_user_counts
    "#{project_folder}/data/user_counts/"
  end

  def daily_user_starts
    "#{project_folder}/data/user_starts/"
  end

  def extract_edges(day)
    edges = []
    CSV.open(time_transitions+"/"+day).each do |row|
      edges << [row[0], row[1]]
    end;false
    csv = CSV.open(daily_edges_file+day, "w")
    edges.uniq.each do |edge|
      csv << edge
    end;false
    csv.close
  end

  def extract_self_loops(day)
    self_loops = {}
    CSV.open(time_transitions+"/"+day).each do |row|
      self_loops[row[0]] ||= {yes: 0, no: 0}
      if row[0] == row[1]
        self_loops[row[0]][:yes] += 1
      else
        self_loops[row[0]][:no] += 1
      end
    end;false
    csv = CSV.open(daily_self_loops+day, "w")
    self_loops.each do |subreddit, loop_data|
      csv << [subreddit, loop_data[:yes].to_f/(loop_data[:yes]+loop_data[:no])]
    end;false
    csv.close
  end

  def extract_user_counts(day)
    user_counts = {}
    CSV.open(time_transitions+"/"+day).each do |row|
      user_counts[row.last] ||= 0
      user_counts[row.last] += 1
    end;false
    csv = CSV.open(daily_user_counts+day, "w")
    user_counts.each do |user, count|
      csv << [user, count]
    end;false
    csv.close
  end

  def extract_user_starts(day)
    user_starts = {}
    CSV.open(time_transitions+"/"+day).each do |row|
      if user_starts[row.last].nil?
        user_starts[row.last] = row.first
      end
    end;false
    csv = CSV.open(daily_user_starts+day, "w")
    user_starts.each do |user, start|
      csv << [user, start]
    end;false
    csv.close
  end
end