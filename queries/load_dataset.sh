#!/bin/sh

# Get the data.
rm -f all.tsv all.tsv.bz2 'all.tsv.bz2?raw=true'
wget https://github.com/cartershanklin/hive-spatial-uber/blob/master/data/all.tsv.bz2?raw=true
mv 'all.tsv.bz2?raw=true' all.tsv.bz2
bunzip2 all.tsv.bz2

# Load in Hive.
cat<<EOF>load.sql
drop table if exists uber;
drop table if exists uber_orc;
create table uber (
	id int,
	ts string,
	long double,
	lat double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
load data local inpath 'all.tsv' overwrite into table uber;
create table uber_orc stored as orc as select * from uber;
EOF

hive -f load.sql

cat<<EOF>query.sql
select sub1.id, sub2.id, sub1.long, sub1.lat, sub2.long, sub2.lat, sqrt( pow(sub1.long - sub2.long, 2) + pow(sub1.lat - sub2.lat, 2) ) * 111 as distance_km
from
( select id, long, lat, row_number() over ( partition by id order by ts desc ) as rn from uber_orc ) sub1,
( select id, long, lat, row_number() over ( partition by id order by ts desc ) as rn from uber_orc ) sub2
where sub1.rn = 1 and sub2.rn = 1 and
sqrt( pow(sub1.long - sub2.long, 2) + pow(sub1.lat - sub2.lat, 2) ) <= (1/(111 * 10));
EOF
