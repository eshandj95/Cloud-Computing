drop table g1;

create table g1(
s_g1 BIGINT,
d_g1 BIGINT)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table g1;

INSERT OVERWRITE TABLE g1
SELECT d_g1,count(*)
FROM g1
GROUP BY d_g1;

SELECT s_g1,d_g1
FROM g1
SORT BY d_g1 desc;
