g1=LOAD '$G' USING PigStorage(',') AS (p1:long, p2:long);
g2=GROUP g1 by p2;
g3=FOREACH g2 GENERATE group, COUNT(g1);
g4 = ORDER g3 BY $1 DESC;
STORE g4 INTO '$O' USING PigStorage(',');