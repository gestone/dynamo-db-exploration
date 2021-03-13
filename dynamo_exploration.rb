require 'aws-sdk-dynamodb'

=begin
  Problems, data set, everything in here:

  https://gitlab.cs.washington.edu/suciu/cse344-2019wi/blob/master/hw/hw2/hw2.md

  SQL schema:

  FLIGHTS (fid int,
           month_id int,        -- 1-12
           day_of_month int,    -- 1-31
           day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
           carrier_id varchar(7),
           flight_num int,
           origin_city varchar(34),
           origin_state varchar(47),
           dest_city varchar(34),
           dest_state varchar(46),
           departure_delay int, -- in mins
           taxi_out int,        -- in mins
           arrival_delay int,   -- in mins
           canceled int,        -- 1 means canceled
           actual_time int,     -- in mins
           distance int,        -- in miles
           capacity int,
           price int            -- in $
           )

  CARRIERS (cid varchar(7), name varchar(83))
  MONTHS (mid int, month varchar(9))
  WEEKDAYS (did int, day_of_week varchar(9))

  The goal of this exercise is to see how we can use Dynamo DB to solve the following problems
  with Dynamo best practices in this video here:

  https://www.youtube.com/watch?v=6yqfmXiZTlM

=end
@ddb_client = Aws::DynamoDB::Client.new(endpoint: 'http://localhost:8000')

def create_table
  @ddb_client.create_table({
    # TODO: Fill this in
  })
end

=begin
  1. List the distinct flight numbers of all flights from Seattle to Boston by Alaska Airlines Inc. on Mondays.
  Also notice that, in the database, the city names include the state. So Seattle appears as Seattle WA.

  [Hint: Output relation cardinality: 3 rows]
=end
def query_one
  @ddb_client.query()
end

=begin
  Find all itineraries from Seattle to Boston on July 15th. Search only for itineraries that have one stop (i.e., flight 1: Seattle -> [somewhere], flight2: [somewhere] -> Boston).
  Both flights must depart on the same day (same day here means the date of flight) and must be with the same carrier. It's fine if the landing date is different from the departing date (i.e., in the case of an overnight flight). You don't need to check whether the first flight overlaps with the second one since the departing and arriving time of the flights are not provided.
  The total flight time (actual_time) of the entire itinerary should be fewer than 7 hours (but notice that actual_time is in minutes).
  For each itinerary, the query should return the name of the carrier, the first flight number,
  the origin and destination of that first flight, the flight time, the second flight number,
  the origin and destination of the second flight, the second flight time, and finally the total flight time.
  Only count flight times here; do not include any layover time.
  Name the output columns name as the name of the carrier, f1_flight_num, f1_origin_city, f1_dest_city, f1_actual_time, f2_flight_num, f2_origin_city, f2_dest_city, f2_actual_time, and actual_time as the total flight time. List the output columns in this order.
  [Output relation cardinality: 1472 rows]
=end
def query_two
  @ddb_client.query()
end

=begin
  Find the day of the week with the longest average arrival delay.
  Return the name of the day and the average delay.
  Name the output columns day_of_week and delay, in that order. (Hint: consider using LIMIT. Look up what it does!)
  [Output relation cardinality: 1 row]
=end
def query_three
  @ddb_client.query()
end

=begin
  Find the names of all airlines that ever flew more than 1000 flights in one day
  (i.e., a specific day/month, but not any 24-hour period).
  Return only the names of the airlines. Do not return any duplicates
  (i.e., airlines with the exact same name).
  Name the output column name.
  [Output relation cardinality: 12 rows]
=end
def query_four
  @ddb_client.query()
end

=begin
  (10 points) Find all airlines that had more than 0.5 percent of their flights out of Seattle be canceled.
  Return the name of the airline and the percentage of canceled flight out of Seattle.
  Order the results by the percentage of canceled flights in ascending order.
  Name the output columns name and percent, in that order.
  [Output relation cardinality: 6 rows]
=end
def query_five
  @ddb_client.query()
end

=begin
  (10 points) Find the maximum price of tickets between Seattle and New York, NY (i.e. Seattle to NY or NY to Seattle).
  Show the maximum price for each airline separately.
  Name the output columns carrier and max_price, in that order.
  [Output relation cardinality: 3 rows]
=end
def query_six
  @ddb_client.query()
end

=begin
  (10 points) Find the total capacity of all direct flights that fly between Seattle and San Francisco, CA on July 10th (i.e. Seattle to SF or SF to Seattle).
  Name the output column capacity.
  [Output relation cardinality: 1 row]
=end
def query_seven
  @ddb_client.query()
end

=begin
  (10 points) Compute the total departure delay of each airline
  across all flights.
  Name the output columns name and delay, in that order.
  [Output relation cardinality: 22 rows]
=end
def query_eight
  @ddb_client.query()
end


