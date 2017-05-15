module RedisNetworkHelper
  def neighbors(node, network, direction="out")
    if direction == "out"
      network["edges"].select{|e| e[0] == node}.collect(&:last).uniq
    elsif direction == "in"
      network["edges"].select{|e| e[1] == node}.collect(&:first).uniq
    elsif direction == "both"
      network["edges"].select{|e| e[0] == node}.collect(&:last)|network["edges"].select{|e| e[1] == node}.collect(&:first)
    end
  end

  def set_neighbors(node, network, direction="out")
    neighbors = neighbors(node, network, direction)
    $redis.del(@memory_prefix+"node_neighbors"+node+direction)
    $redis.lpush(@memory_prefix+"node_neighbors"+node+direction, neighbors) if neighbors.length > 0
  end

  def get_neighbors(node, direction="out")
    length = $redis.llen(@memory_prefix+"node_neighbors"+node+direction)
    if length == 0
      []
    else
      $redis.lrange(@memory_prefix+"node_neighbors"+node+direction, 0,length-1)
    end
  end

  def set_histories(user, user_histories)
    $redis.del(@memory_prefix+"user_histories"+user)
    $redis.lpush(@memory_prefix+"user_histories"+user, user_histories)
  end

  def get_histories(user)
    length = $redis.llen(user)
    $redis.lrange(@memory_prefix+"user_histories"+user, 0,length-1)
  end
  
  def clear_latest_histories(user)
    $redis.del(@memory_prefix+"user_latest_histories"+user)
  end

  def set_latest_histories(user, user_histories)
    $redis.del(@memory_prefix+"user_latest_histories"+user)
    $redis.lpush(@memory_prefix+"user_latest_histories"+user, user_histories)
  end

  def get_latest_histories(user)
    length = $redis.llen(user)
    $redis.lrange(@memory_prefix+"user_latest_histories"+user, 0,length-1)
  end

  def store_network(network, direction="out")
    network["nodes"].each do |node|
      set_neighbors(node, network, direction)
    end
  end

  def clear_network(network, direction="out")
    network["nodes"].each do |node|
      $redis.del(@memory_prefix+"node_neighbors"+node+direction)
    end
  end

  def store_metadata(name, data)
    $redis.set(@memory_prefix+"metadata"+name, data.to_json)
  end

  def get_metadata(name)
    JSON.parse($redis.get(@memory_prefix+"metadata"+name))
  end
  
  def clear_metadata(name)
    $redis.del(@memory_prefix+"metadata"+name)
  end
end