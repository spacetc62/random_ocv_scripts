require 'rubygems'
require "bundler/setup"
require 'faster_csv'
require 'whois'
require 'ruby-debug'
require 'andand'
require 'domainatrix'
require 'postrank-uri'

stores = ["HH"]

root_path = "/Users/mkuehl/Dropbox/Link Data/MASTER LINK LISTS/"

whois = Whois::Client.new

domains = { }

stores.each do |store|
  FasterCSV.open("#{root_path}/#{store}_domains_with_contact.csv", "w") do |csv|
    csv << ["Domain", "Admin Contact Name", "Admin Contact Organization", "Admin Contact Email",
            "Registrant Contact Name", "Registrant Contact Organization", "Registrant Contact Email",
            "Technical Contact Name", "Technical Contact Organization", "Technical Contact Email"]
    FasterCSV.foreach("#{root_path}/#{store}_domains.csv", :headers => true) do |row|
      domain = nil
      begin
        r = Domainatrix.parse(PostRank::URI.clean(row["Domain"]))
        domain = "#{r.domain}.#{r.public_suffix}"
      rescue
        domain = row["Domain"]
      end
      next unless domain
      
      unless domains[domain]
        begin
          r = whois.query(domain)
          sleep 1
          domains[domain] = [r.admin_contact.andand.name, r.admin_contact.andand.organization,
                             r.admin_contact.andand.email, r.registrant_contact.andand.name,
                             r.registrant_contact.andand.organization, r.registrant_contact.andand.email,
                             r.technical_contact.andand.name, r.technical_contact.andand.organization,
                             r.technical_contact.andand.email]
        rescue Object => e
          puts e.inspect
          domains[domain] = []
        end
      end
      csv << [row["Domain"]] + domains[domain]      
      puts "#{store}: #{domain}"
    end
    sleep 5
  end
end
