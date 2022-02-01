CREATE TABLE Person (
  name VARCHAR(255) NOT NULL,
  picture VARBINARY(255),
  sex VARCHAR(255),
  birthdate TIMESTAMP,
  accepted_tos BOOLEAN,
  notused VARCHAR(255),
  PRIMARY KEY (name)
);
