require "directory_server/version"
require 'threadpool'

module DirectoryServer
  class Server

    def self.run(options)
      pool = Threadpool::Threadpool.new workers: 4
      server = TCPServer.new("localhost", options[:port])
      puts "Directory server listening on port #{options[:port]}"
      @@fileservers = {}
      @@files = {}
      loop do
        client = server.accept
        puts "Talking to client: #{client}"
        pool.add_task(self.process_request, self, client, options[:peers])
      end
    end

    def add_file_server(name, host, port)
      @@fileservers[name] = { :host => host, :port => port }
    end

    def self.process_request()
      Proc.new do |server, client|

        puts "Processing request for: #{client}"

        request = client.gets.strip
        puts "Request: #{request}"
        command_word = request.split()[0]

        case command_word

        when "PING"
          name = request.split()[1].split('=')[1]
          host = request.split()[2].split('=')[1]
          port = request.split()[3].split('=')[1]
          server.add_file_server name, host, port
          client.write "PONG"
        when "SEARCH"
          server_name = request.split()[1].split('=')[1]
          if @@fileservers.include? server_name
            file_name = request.split()[2].split('=')[1]
            file_hash = Digest::MD5.hexdigest(file_name)
            if @@files.include? file_hash
              #TODO ping peers
              peers = files[file_hash]
              peer = @@fileservers[peers[0]][:host] + ":" + @@fileservers[peers[0]][:port]
              client.write "LOCATION PEER=#{peer}"
            else
              client.write "ERROR MESSAGE=FileNotFound"
            end
          else
            client.write "ERROR MESSAGE=WhoAreYou?"
          end
        when "INVALIDATE"
          server_name = request.split()[1].split('=')[1]
          if @@fileservers.include? server_name
            file_name = request.split()[2].split('=')[1]
            file_hash = Digest::MD5.hexdigest(file_name)
            
            if @@files.include? file_hash
              @@files[file_hash].each do |srv|
                unless srv == server_name
                  TCPSocket.new @@fileservers[server_name][:host], @@fileservers[server_name][:port] do |sock|
                    sock.write "INVALIDATE FILE=#{file_name}"
                  end
                end
              end
              @@files.remove(file_hash)
            end
          else
            client.write "ERROR MESSAGE=WhoAreYou?"
          end
        when "REPLICATE"
          server_name = request.split()[1].split('=')[1]
          if @@fileservers.include? server_name
            file_name = request.split()[2].split('=')[1]
            file_hash = Digest::MD5.hexdigest(file_name)
            if @@files.include? file_hash
              servers = (Set.new @@fileservers.keys) - Set.new(@@files[file_hash]) - (Set.new server_name)
              servers = servers.to_a
            else
              @@files[file_hash] << server_name
              servers = (Set.new @@fileservers.keys) - (Set.new server_name)
              servers = servers.to_a
            end
            servers = servers.sample(((servers.size-1)/2)+1)
            ss = servers.map do |m| 
              @@fileservers[m][:host] + ":" + @@fileservers[m][:port]
            end
            client.write "PEERS LIST=#{ss.join(",")}"
          else
            client.write "ERROR MESSAGE=WhoAreYou?"
          end

        when "REGISTER"
          server_name = request.split()[1].split('=')[1]
          if @@fileservers.include? server_name
            file_name = request.split()[2].split('=')[1]
            file_hash = Digest::MD5.hexdigest(file_name)
            @@files[file_hash] << server_name
            client.write "STATUS=OKAY"
          else
            client.write "ERROR MESSAGE=WhoAreYou?"
          end
        end
        client.close
      end
    end
  end
end