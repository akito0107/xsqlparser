-- from https://github.com/isucon/isucon8-qualify/blob/master/db/schema.sql

CREATE TABLE IF NOT EXISTS users (
    id          INTEGER UNSIGNED PRIMARY KEY AUTO_INCREMENT,
    nickname    VARCHAR(128) NOT NULL,
    login_name  VARCHAR(128) NOT NULL,
    pass_hash   VARCHAR(128) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET utf8mb4;
