require 'net/http'

HOST = "http://127.0.0.1:#{OSRM_PORT}"
DESTINATION_REACHED = 15      #OSRM instruction code

def request_path service, params
  uri = "#{HOST}/" + service
  response = send_request uri, params
  return response
end

def request_url path
  uri = URI.parse"#{HOST}/#{path}"
  @query = uri.to_s
  Timeout.timeout(OSRM_TIMEOUT) do
    Net::HTTP.get_response uri
  end
rescue Errno::ECONNREFUSED => e
  raise "*** osrm-routed is not running."
rescue Timeout::Error
  raise "*** osrm-routed did not respond."
end

def merge_params params, other
  merged = {}
  params.each do |k,v|
    if other.has_key? k then
      merged[k] = params[k].concat other[k]
    else
      merged[k] = params[k]
    end
  end
  other.each do |k,v|
      merged[k] = other[k]
  end

  return merged
end

def request_route waypoints, user_params
  defaults = { 'output' => ['json'], 'instructions' => [true], 'alt' => [false] }
  params = merge_params defaults, user_params
  params[:loc] = waypoints.map{ |w| "#{w.lat},#{w.lon}" }

  return request_path "viaroute", params
end

def request_locate node, user_params
  defaults = { 'output' => ['json'] }
  params = merge_params defaults, user_params
  params[:loc] = ["#{node.lat},#{node.lon}"]

  return request_path "locate", params
end

def request_nearest node, user_params
  defaults = { 'output' => ['json'] }
  params = merge_params defaults, user_params
  params[:loc] = ["#{node.lat},#{node.lon}"]

  return request_path "nearest", params
end

def request_table waypoints, user_params
  defaults = { 'output' => ['json'] }
  params = merge_params defaults, user_params
  locs = waypoints.select{ |w| w[:type] == "loc"}.map { |w| "#{w[:coord].lat},#{w[:coord].lon}" }
  dsts = waypoints.select{ |w| w[:type] == "dst"}.map { |w| "#{w[:coord].lat},#{w[:coord].lon}" }
  srcs = waypoints.select{ |w| w[:type] == "src"}.map { |w| "#{w[:coord].lat},#{w[:coord].lon}" }
  if locs.size > 0
    params[:loc] = locs
  end
  if dsts.size > 0
    params[:dst] = dsts
  end
  if srcs.size > 0
    params[:src] = srcs
  end

  return request_path "table", params
end

def request_trip waypoints, user_params
  defaults = { 'output' => ['json'] }
  params = merge_params defaults, user_params
  params[:loc] = waypoints.map{ |w| "#{w.lat},#{w.lon}" }

  return request_path "trip", params
end

def request_matching waypoints, timestamps, user_params
  defaults = { 'output' => ['json'] }
  params = merge_params defaults, user_params
  params[:loc] = waypoints.map{ |w| "#{w.lat},#{w.lon}" }
  if timestamps.size > 0
    params[:t] = timestamps
  end

  return request_path "match", params
end

def got_route? response
  if response.code == "200" && !response.body.empty?
    json = JSON.parse response.body
    if json['status'] == 0
      return way_list( json['route_instructions']).empty? == false
    end
  end
  return false
end

def route_status response
  if response.code == "200" && !response.body.empty?
    json = JSON.parse response.body
    if json['status'] == 0
      if way_list(json['route_instructions']).empty?
        return 'Empty route'
      else
        return 'x'
      end
    elsif json['status'] == 207
      ''
    else
      "Status #{json['status']}"
    end
  else
    "HTTP #{response.code}"
  end
end

def extract_instruction_list instructions, index, postfix=nil
  if instructions
    instructions.reject { |r| r[0].to_s=="#{DESTINATION_REACHED}" }.
    map { |r| r[index] }.
    map { |r| (r=="" || r==nil) ? '""' : "#{r}#{postfix}" }.
    join(',')
  end
end

def way_list instructions
  extract_instruction_list instructions, 1
end

def compass_list instructions
  extract_instruction_list instructions, 6
end

def bearing_list instructions
  extract_instruction_list instructions, 7
end

def turn_list instructions
  if instructions
    types = {
      0 => :none,
      1 => :straight,
      2 => :slight_right,
      3 => :right,
      4 => :sharp_right,
      5 => :u_turn,
      6 => :sharp_left,
      7 => :left,
      8 => :slight_left,
      9 => :via,
      10 => :head,
      11 => :enter_roundabout,
      12 => :leave_roundabout,
      13 => :stay_roundabout,
      14 => :start_end_of_street,
      15 => :destination,
      16 => :enter_contraflow,
      17 => :leave_contraflow
    }
    # replace instructions codes with strings
    # "11-3" (enter roundabout and leave a 3rd exit) gets converted to "enter_roundabout-3"
    instructions.map do |r|
      r[0].to_s.gsub(/^\d*/) do |match|
        types[match.to_i].to_s
      end
    end.join(',')
  end
end

def mode_list instructions
  extract_instruction_list instructions, 8
end

def time_list instructions
  extract_instruction_list instructions, 4, "s"
end

def distance_list instructions
  extract_instruction_list instructions, 2, "m"
end
