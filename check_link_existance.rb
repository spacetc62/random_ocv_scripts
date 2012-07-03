require 'rubygems'
require "bundler/setup"
require 'faster_csv'
require 'postrank-uri'
require 'uri'
require 'ruby-debug'
#require 'active_support'
require 'domainatrix'
require 'mechanize'

to_check = []

FasterCSV.foreach("/Users/mkuehl/Dropbox/Link\ Data/SW\ Reinclusion/Do_these_links_still_exist.csv", :headers => true) do |row|
  to_check << row[0]
end

master_set = []
puts to_check.size
until to_check.size == 0
  checking_now = to_check.pop(250)
  threads = checking_now.map do |d|
    source = d
    Thread.new(d) do |source|
      m = Mechanize.new
      m.user_agent_alias = "Linux Firefox"
      result = nil
      final_url = nil
      begin
        page = m.get(source)
        result = 0
        result += page.body.to_s.scan("://www.sunglasswarehouse.com").size
        result += page.body.to_s.scan("://sunglasswarehouse.com").size
      rescue Mechanize::ResponseCodeError => e
        result = "#{e.response_code} STATUS"
      rescue Object => e
        result = e.class.to_s
      end
      master_set << [source, result]
    end
  end
  threads.each { |thread| thread.join }
  puts to_check.size
end
 
FasterCSV.open("blah_output_with_check.csv", "w") do |csv|
  csv << ["Sample Linking Page from Domain","Number of Links to SW"]
  master_set.each do |v|
    csv << v
  end
end
