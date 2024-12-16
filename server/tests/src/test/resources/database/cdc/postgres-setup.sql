CREATE SCHEMA cdc;
CREATE TABLE cdc.department(
   id INT NOT NULL,
   name VARCHAR(45),
   PRIMARY KEY(id)
);

CREATE TABLE cdc.student(
   id INT NOT NULL,
   name VARCHAR(45),
   grad_year VARCHAR(4),
   major_id INT NOT NULL,
   PRIMARY KEY(id),
   FOREIGN KEY (major_id) REFERENCES cdc.department(id)
);
