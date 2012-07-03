require 'rubygems'
require 'faster_csv'
require 'ruby-debug'

stores = ["TFS", "RGS", "TSMO", "ST", "SW"]

root_path = "/Users/mkuehl/Dropbox/Link Data/MASTER LINK LISTS/"

domains = { }
stores.each do |store|
  FasterCSV.foreach("#{root_path}/#{store}_domains.csv", :headers => true) do |row|
    domains[row["Domain"]] ||= { }
    domains[row["Domain"]][store] = "YES"
  end
end

FasterCSV.open("common_domains.csv", "w") do |csv|
  csv << ["Domain"] + stores
  domains.select{ |k,v| v.size > 1 }.sort_by{ |k,v| v.size }.reverse.each do |k, v|
    csv << [k] + stores.map{ |r| v[r] }
  end
end

urls = { }
stores.each do |store|
  FasterCSV.foreach("#{root_path}/#{store}_output_with_check.csv", :headers => true) do |row|
    url = row["Redirected To"]
    url = row["Source URL"] if url.nil? || url.strip == ""
    urls[url] ||= { }
    urls[url][store] = "YES"
    urls[url][:valid] ||= row["Link Check"] == "LINK"
  end
end

FasterCSV.open("common_urls.csv", "w") do |csv|
  csv << ["URL", "Valid"] + stores
  urls.select{ |k,v| v.size > 2 }.sort_by{ |k,v| v.size }.reverse.each do |k, v|
    csv << [k, v[:valid]] + stores.map{ |r| v[r] }
  end
end
