DROP TABLE records IF EXISTS;

CREATE TABLE records  (
    token VARCHAR(50),
    publishTimestamp TIMESTAMP,
    payload CLOB,
    type VARCHAR(20)
);