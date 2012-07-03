require 'rubygems'
require "bundler/setup"
require 'faster_csv'
require 'postrank-uri'
require 'uri'
require 'ruby-debug'
#require 'active_support'
require 'domainatrix'
require 'mechanize'

FILE_LIST = { 
  :SW => {
    :majestic => "majesticfreshindex/F-SW-majestic.csv",
    :ose => "OSE/Sunglasses Warehouse OSE.csv",
    :wmt => "WMT/All_Links_sunglasswarehouse_com_20120419T164753Z.csv",
    :wmt_www => "WMT/2_All_Links_www_sunglasswarehouse_com_20120419T164548Z.csv",
    :regexp => /sunglasswarehouse/i
  },
  :RGS => {
    :majestic => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/RGS 2nd time/majestic.csv",
    :ose => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/RGS 2nd time/OSE.csv",
    :wmt => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/RGS 2nd time/webmastertools.csv",
    :regexp => /readingglassesshopper/i
  },
  :TFS => {
    :majestic => "majesticfreshindex/F-TFS-majestic.csv",
    :ose => "OSE/Fedora Store OSE.csv",
    :wmt => "WMT/All_Links_www_thefedorastore_com_20120419T164343Z.csv",
    :regexp => /thefedorastore/i
  },
  :TSMO => {
    :majestic => "majesticfreshindex/F-tsmo-majestic.csv",
    :ose => "OSE/Sunglasses Man OSE.csv",
    :wmt => "WMT/All_Links_www_thesunglassmanonline_com_20120419T165057Z.csv",
    :regexp => /thesunglassmanonline/i
  },
  :ST => {
    :majestic => "majesticfreshindex/F-silvertreats-majestic.csv",
    :ose => "OSE/Silver Treats OSE.csv",
    :wmt => "WMT/All_Links_www_silvertreats_com_20120419T163902Z.csv",
    :regexp => /silvertreats/i
  },
  :HH => { 
    :majestic => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/HandbagHeaven/HH_backlink_urls_Majestic_SEO.csv",
    :ose => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/HandbagHeaven/HH_backlink_urls_ose.csv",
    :wmt => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/HandbagHeaven/HH_backlink_urls_webmaster_tools.csv",
    :regexp => /handbagheaven/i
  },
  :SDN => { 
    :majestic => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/SDN/CSVs 5.13.2012/SDN_Majestic_backlinks.csv",
    :ose => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/SDN/CSVs 5.13.2012/SDN_ose_backlinks.csv",
    :wmt => "/Users/mkuehl/Dropbox/Link Data/Backlink lists/SDN/CSVs 5.13.2012/SDN_webmasterTools_backlinks.csv",
    :regexp => /scarves/i
  }
}

class Hash
  def slice(*keys)
    keys = keys.map! { |key| convert_key(key) } if respond_to?(:convert_key)
    hash = self.class.new
    keys.each { |k| hash[k] = self[k] if has_key?(k) }
    hash
  end
end

STORE_TO_RUN = :RGS

master_set = { }

master_headers = ["Target URL", "Source URL", "Source ACRank", "DA", "Anchor Text",
                  "Source First Found Date", "FlagNoFollow", "Link Check", "Redirected To"]

conversions = { "\357\273\277SourceURL" => "Source URL",
  "SourceURL" => "Source URL",
  "AnchorText" => "Anchor Text",
  "TargetURL" => "Target URL",
  "ACRank" => "Source ACRank",
  "Date" => "Source First Found Date" }

FasterCSV.foreach(FILE_LIST[STORE_TO_RUN][:majestic], :headers => true) do |row|
  vals = row.to_hash
  conversions.each { |k,v| vals[v] = vals.delete(k) if vals.has_key?(k) }
  vals = vals.slice(*master_headers)
  next unless vals["Source URL"]
  vals["Source URL"] = PostRank::URI.clean(vals["Source URL"])
  if master_set[vals["Source URL"]]
    append = 1
    append += 1 until !master_set[vals["Source URL"]+"##{append}"]
    vals["Source URL"] = vals["Source URL"] + "##{append}"
  end
  master_set[vals["Source URL"]] = vals
end
puts "Majestic: #{master_set.size}"

match_count = 0
FasterCSV.foreach(FILE_LIST[STORE_TO_RUN][:ose], :headers => true) do |vals|
  next unless vals["URL"]
  url = PostRank::URI.clean(vals["URL"])
  match = master_set[url]

  if match
    match["DA"] = vals["DA"] || vals["Domain Authority"]
    match_count += 1
  else
    master_set[url] = { 
      "Source URL"  => url,
      "DA"          => vals["DA"] || vals["Domain Authority"],
      "Anchor Text" => vals["Anchor Text"] }
  end
end
puts "Plus OSE: #{master_set.size}"
puts "OSE Match: #{match_count}"

match_count = 0
FasterCSV.foreach(FILE_LIST[STORE_TO_RUN][:wmt], :headers => true) do |row|
  url = PostRank::URI.clean(row["Links"])
  match = master_set[url]
  if match
    match_count += 1
  else
    master_set[url] = { "Source URL" => url }
  end
end
puts "Plus WMT: #{master_set.size}"
puts "WMT Match: #{match_count}"

if FILE_LIST[STORE_TO_RUN][:wmt_www]
  match_count = 0
  FasterCSV.foreach(FILE_LIST[STORE_TO_RUN][:wmt_www], :headers => true) do |row|
    url = PostRank::URI.clean(row["Links"])
    match = master_set[url]
    if match
      match_count += 1
    else
      master_set[url] = { "Source URL" => url }
    end
  end
  puts "Plus WMT www: #{master_set.size}"
  puts "WMT www Match: #{match_count}"
end

to_check = master_set.keys.to_a
 
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
        if page.body.to_s =~ FILE_LIST[STORE_TO_RUN][:regexp]
          result = "LINK"
        else
          result = "NOLINK"
        end
        final_url = page.uri.to_s if page.uri.to_s != source
      rescue Mechanize::ResponseCodeError => e
        result = "#{e.response_code} STATUS"
      rescue Object => e
        result = e.class.to_s
      end
      master_set[source]["Link Check"] = result
      master_set[source]["Redirected To"] = final_url if final_url
    end
  end
  threads.each { |thread| thread.join }
  puts to_check.size
end
 
FasterCSV.open("#{STORE_TO_RUN}_output_with_check.csv", "w") do |csv|
  csv << master_headers
  master_set.keys.sort.each do |key|
    vals = master_set[key]
    csv << master_headers.map{ |h| vals[h] }
  end
end

domains = { }
master_set.keys.each do |source|
  domain = nil
  begin
    r = Domainatrix.parse(source)
    domain = "#{r.domain}.#{r.public_suffix}"
    domain = "#{r.subdomain}.#{domain}" unless r.subdomain.blank? || r.subdomain.downcase == "www"
  rescue
    domain = URI.parse(source).host rescue nil
  end
  domains[domain] ||= 0
  domains[domain] += 1
end
puts "Domain Count: #{domains.size}"

FasterCSV.open("#{STORE_TO_RUN}_domains.csv", "w") do |csv|
  csv << ["Domain", "Count of Links"]
  domains.to_a.sort_by(&:last).reverse.each do |v|
    csv << v
  end
end
