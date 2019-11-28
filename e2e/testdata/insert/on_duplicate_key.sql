-- from: https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
INSERT INTO t1 (a,b,c) VALUES (1,2,3) ON DUPLICATE KEY UPDATE c=c+1;
