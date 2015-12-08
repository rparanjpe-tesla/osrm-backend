require 'net/http'

# Converts a hash of "param" => ["val1", "val2"] into ?param=val1&param=val2
def params_to_url params
  kv_pairs = []
  params.each do |param,values|
    kv_pairs.concat values.map { |value| param.to_s + "=" + value.to_s }
  end
  url = "?" + kv_pairs.join("&")
  return url
end

def send_request base_uri, parameters
  Timeout.timeout(OSRM_TIMEOUT) do
    if @http_method.eql? "POST"
      uri = URI.parse base_uri
      @query = uri.to_s
      response = Net::HTTP.post_form uri, parameters
    else
      uri = URI.parse base_uri+(params_to_url parameters)
      @query = uri.to_s
      response = Net::HTTP.get_response uri
    end
  end
rescue Errno::ECONNREFUSED => e
  raise "*** osrm-routed is not running."
rescue Timeout::Error
  raise "*** osrm-routed did not respond."
end
