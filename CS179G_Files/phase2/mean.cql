CREATE KEYSPACE mean WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 3};

CREATE TABLE mean.test (
  RegionID float PRIMARY KEY,
  RegionName varchar,
  State varchar,
  CountyName varchar,
  SizeRank float,
  Mean float
);


