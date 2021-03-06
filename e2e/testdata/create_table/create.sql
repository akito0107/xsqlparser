CREATE TABLE test (
    col1 int primary key,
    col2 char(10),
    col3 VARCHAR,
    col4 VARCHAR(255),
    col5 uuid NOT NULl,
    col6 smallint check(col6 < 10),
    col7 bigint UNIQUE,
    col8 integer constraint test_constraint check (10 < col8 and col8 < 100),
    col9 serial,
    col10 character varying,
    col11 real references test2(col1),
    col12 double precision,
    col13 date,
    col14 time,
    col15 timestamp default current_timestamp,
    col16 boolean default false,
    col17 numeric(10, 10),
    col18 text,
    foreign key (col1, col2) references test2(col1, col2),
    unique key (col1, col2),
    CONSTRAINT test_constraint check(col1 > 10)
)