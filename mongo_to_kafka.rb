require 'aws-sdk'
require 'mongo'
require 'poseidon'
require 'json'

if (ARGV.length < 2)
  p  "Fuck You"
end

project = ARGV[0]
brokers = ARGV[1].split(',')
download = !(ARGV[2] == 'false')
delete = !(ARGV[3] == "false")

# Download Database
date = `date +%Y-%m-%d`.chomp

if download
  AWS.config(access_key_id: ENV['S3_ACCESS_ID'], secret_access_key: ENV['S3_SECRET_KEY'])
  s3 = AWS::S3.new
  bucket = s3.buckets['zooniverse-code']
  obj = bucket.objects["databases/#{date}/ouroboros_projects/#{project}_#{date}.tar.gz"]

  File.open("dump.tar.gz", "wb") do |file|
    obj.read do |chunk|
      file.write(chunk)
    end
  end

  `tar xzf dump.tar.gz`
  `mongorestore --db #{project} #{project}_#{date}/`
end

def debsonify(hash)
  hash.each do |key, value|
    if key =~ /_ids/
      hash[key] = value.map{|id| id.to_s}
    elsif key =~ /_id/
      hash[key] = value.to_s
    elsif value.is_a?(Hash) || value.is_a?(Array)
      hash[key] = debsonify(value)
    end
  end
end

def format (hash, project)
  subjects = hash["subjects"].map{|s| s["_id"]}
  subjects = hash["subject_ids"] if subjects.empty?
  c = {
    id: hash["_id"],
    project: project,
    subjects: subjects,
    timestamp: hash["created_at"],
    user_ip: hash["user_ip"],
    annotations: hash["annotations"]
  }

  unless hash["user_name"].nil?
    c[:user] = hash["user_name"]
    c[:user_id] = hash["user_id"]
  end

  return c
end

kafka_prod = Poseidon::Producer.new(brokers, "mongo_kafka")

client = Mongo::MongoClient.new('localhost', '27017')
cs = client[project]["#{project}_classifications"]

count = File.read(".offset").to_i || 0

cs.find({}, :timeout => false) do |cursor|
  cursor.skip(count) unless count == 0

  cursor.each do |doc|
    debsonify(doc)
    doc = format(doc, project)
    kafka_prod.send_messages([Poseidon::MessageToSend.new("classifications", doc.to_json)])
    count += 1
    File.open(".offset", "w") { |f| f.write(count.to_i) }
  end
end

if delete 
  `mongo #{project} --eval "db.dropDatabase();"`
  `rm -rf #{project}_#{date}`
  `rm -rf dump.tar.gz`
end
