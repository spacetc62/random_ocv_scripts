require 'rubygems'
require 'faster_csv'
require 'postrank-uri'
require 'uri'
require 'ruby-debug'
#require 'active_support'
require 'domainatrix'

stores = ["ABC"]

root_path = "/Users/mkuehl/Dropbox/Link Data/MASTER LINK LISTS"

stores.each do |store|
  FasterCSV.open("#{root_path}/#{store}_domains_with_ip.csv", "w") do |csv|
    csv << ["Domain", "IP"]
    FasterCSV.foreach("#{store}_domains.csv", :headers => true) do |row|
      source = PostRank::URI.clean(row["Domain"])
      domain = nil
      begin
        r = Domainatrix.parse(source)
        domain = "#{r.domain}.#{r.public_suffix}"
        domain = "#{r.subdomain}.#{domain}" unless r.subdomain.blank? || r.subdomain.downcase == "www"
      rescue
        domain = URI.parse(source).host rescue nil
      end
      next unless domain
      ip = Socket.getaddrinfo(domain, 80, nil, Socket::SOCK_STREAM)[0][3] rescue nil
      puts "#{domain} #{ip}"
      csv << [domain, ip.to_s]
    end
  end
end
