CREATE KEYSPACE occuWage WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE occuWage.series (
  series_id varchar PRIMARY KEY,
  areatype_code	varchar,
  industry_code varchar,
  occupation_code varchar,	
  datatype_code	varchar,
  state_code varchar,
  area_code varchar,
  sector_code varchar,	
  series_title varchar,
  begin_year varchar,
  begin_period varchar,
  end_year varchar,
  end_period varchar
);
