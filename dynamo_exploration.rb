require 'aws-sdk-dynamodb'
require 'csv'

# Problems, data set, everything in here:

# https://gitlab.cs.washington.edu/suciu/cse344-2019wi/blob/master/hw/hw2/hw2.md
#
# SQL schema:
#
# FLIGHTS (fid int,
#          month_id int,        -- 1-12
#          day_of_month int,    -- 1-31
#          day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
#          carrier_id varchar(7),
#          flight_num int,
#          origin_city varchar(34),
#          origin_state varchar(47),
#          dest_city varchar(34),
#          dest_state varchar(46),
#          departure_delay int, -- in mins
#          taxi_out int,        -- in mins
#          arrival_delay int,   -- in mins
#          canceled int,        -- 1 means canceled
#          actual_time int,     -- in mins
#          distance int,        -- in miles
#          capacity int,
#          price int            -- in $
#          )
#
# CARRIERS (cid varchar(7), name varchar(83))
# MONTHS (mid int, month varchar(9))
# WEEKDAYS (did int, day_of_week varchar(9))
#
# The goal of this exercise is to see how we can use Dynamo DB to solve the following problems
# with Dynamo best practices in this video here:
#
# https://www.youtube.com/watch?v=6yqfmXiZTlM
@ddb_client = Aws::DynamoDB::Client.new(endpoint: 'http://localhost:8000')
@SERIALIZED_FILE_NAME = 'serialized_data.bin'

def create_table
  @ddb_client.create_table({
    table_name: 'flights',
    key_schema: [

    ],
    attribute_definitions: [

    ],
    provisioned_thoughput: {
      read_capacity_units: 10,
      write_capacity_units: 10
    }
  })
end

def ingest_data
  # TODO: Take flights, carriers, months, and the weekdays tables and shape them to be into Dynamo.
  sql_data = Marshal.load(File.binread(@SERIALIZED_FILE_NAME))
  print "hi"
end

def marshall_data_to_file
  File.open(@SERIALIZED_FILE_NAME, 'wb') {|f| f.write(Marshal.dump(serialize_data_from_csv))}
end

def serialize_data_from_csv
  {
    'flights' => parse_csv(
      'flights-small.csv',
      %w(fid month_id day_of_month day_of_week_id carrier_id 
      flight_num origin_city origin_state dest_city dest_state
      departure_delay taxi_out arrival_delay canceled actual_time distance capacity price)
    ),
    'carriers' => parse_csv('carriers.csv', %w[cid name]),
    'weekdays' => parse_csv('weekdays.csv', %w[did day_of_week])
  }
end

# Takes in a CSV file name and array for which each index of the parsed CSV should map to:
#
# Ex: %w[fid month_id day_of_month day_of_week...]
#
# 
def parse_csv(csv_name, schema)
  CSV.read(csv_name).map {|row| map_row_to_entity(row, schema) }
end

def map_row_to_entity(entity, schema)
  serialized_entity = {}
  schema.zip entity.each do |row|
    serialized_entity[row[0]] = Integer(row[1], exception: false) || row[1]
  end
  serialized_entity 
end


def query_one
  @ddb_client.query()
end

def query_two
  @ddb_client.query()
end

# Find the day of the week with the longest average arrival delay.
# Return the name of the day and the average delay.
# Name the output columns day_of_week and delay, in that order. (Hint: consider using LIMIT. Look up what it does!)
# [Output relation cardinality: 1 row]
def query_three
  @ddb_client.query()
end

# Find the names of all airlines that ever flew more than 1000 flights in one day
# (i.e., a specific day/month, but not any 24-hour period).
# Return only the names of the airlines. Do not return any duplicates
# (i.e., airlines with the exact same name).
# Name the output column name.
# [Output relation cardinality: 12 rows]
def query_four
  @ddb_client.query()
end

# (10 points) Find all airlines that had more than 0.5 percent of their flights out of Seattle be canceled.
# Return the name of the airline and the percentage of canceled flight out of Seattle.
# Order the results by the percentage of canceled flights in ascending order.
# Name the output columns name and percent, in that order.
# [Output relation cardinality: 6 rows]
def query_five
  @ddb_client.query()
end

# (10 points) Find the maximum price of tickets between Seattle and New York, NY (i.e. Seattle to NY or NY to Seattle).
# Show the maximum price for each airline separately.
# Name the output columns carrier and max_price, in that order.
# [Output relation cardinality: 3 rows]
def query_six
  @ddb_client.query()
end

# (10 points) Find the total capacity of all direct flights that fly between Seattle and San Francisco, CA on July 10th (i.e. Seattle to SF or SF to Seattle).
# Name the output column capacity.
# [Output relation cardinality: 1 row]
def query_seven
  @ddb_client.query()
end

# (10 points) Compute the total departure delay of each airline
# across all flights.
# Name the output columns name and delay, in that order.
# [Output relation cardinality: 22 rows]
def query_eight
  @ddb_client.query()
end
