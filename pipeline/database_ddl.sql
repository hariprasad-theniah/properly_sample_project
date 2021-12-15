drop table if exists kaggle_dataset cascade;
create table kaggle_dataset(
 id bigint
,date_ timestamp
,price double precision
,bedrooms smallint
,bathrooms float
,sqft_living integer
,sqft_lot integer
,floors float
,waterfront boolean
,view smallint
,condition smallint
,grade smallint
,sqft_above integer
,sqft_basement integer
,yr_built smallint
,yr_renovated smallint
,zipcode integer
,lat double precision
,long double precision
,sqft_living15 integer
,sqft_lot15 integer
,etl_extract_timestamp timestamp
);
drop table if exists king_county_transit_dataset cascade;
create table king_county_transit_dataset(
 OBJECTID integer
,FEATURE_ID integer
,CODE integer
,NAME_ varchar(128)
,ADDRESS_ varchar(128)
,XCOORD double precision
,YCOORD double precision
,LONGITUDE double precision
,LATITUDE double precision
,ADDRESS_NUM integer
,ON_PREFIX varchar(32)
,ON_STNAME varchar(128)
,ON_STYPE varchar(32)
,ON_SUFFIX varchar(32)
,CROSS_PREFIX  varchar(32)
,CROSS_STNAME varchar(32)
,CROSS_STYPE varchar(32)
,CROSS_SUFFIX varchar(32)
,GEOMETRY_X double precision
,GEOMETRY_Y double precision
,etl_extract_timestamp timestamp
);