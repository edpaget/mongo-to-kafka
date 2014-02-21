require 'aws-sdk'
require 'mongo'
require 'poseidon'
require 'json'

if (ARGV.length < 2)
  p  "Fuck You"
end

project = ARGV[0]
brokers = ARGV[1].split(',')
delete = !(ARGV[2] == "false")

p project, brokers, delete

# Download Database
date = `date +%Y-%m-%d`.chomp

if delete
  AWS.config(access_key_id: ENV['S3_ACCESS_ID'], secret_access_key: ENV['S3_SECRET_KEY'])
  s3 = AWS::S3.new
  bucket = s3.buckets['zooniverse-code']
  p bucket
  p date
  p "databases/#{date}/ouroboros_projects/#{project}_#{date}.tar.gz"
  obj = bucket.objects["databases/#{date}/ouroboros_projects/#{project}_#{date}.tar.gz"]

  File.open("dump.tar.gz", "wb") do |file|
    obj.read do |chunk|
      file.write(chunk)
    end
  end

  `tar xzf dump.tar.gz`
  `mongorestore --db #{project} #{project}_#{date}/`
end

kafka_prod = Poseidon::Producer.new(brokers, "mongo_kafka")

client = Mongo::MongoClient.new('localhost', '27017')
cs = client[project]["#{project}_classifications"]

cs.find({}, :timeout => false) do |cursor|
  cursor.each do |doc|
    doc['project_name'] = project
    sleep(1)
    kafka_prod.send_messages([Poseidon::MessageToSend.new("classifications_#{project}", doc.to_json)])
  end
end

if delete 
  `mongo #{project} --eval "db.dropDatabase();"`
  `rm -rf #{project}_#{date}`
  `rm -rf dump.tar.gz`
end
