SET sql_safe_updates = FALSE;
SET timezone = "UTC";

USE defaultdb;
DROP DATABASE IF EXISTS tpc_w_light CASCADE;
CREATE DATABASE IF NOT EXISTS tpc_w_light;

USE tpc_w_light;

DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS order_line CASCADE;

CREATE TABLE customer(
  c_id UUID PRIMARY KEY,
  c_business_name VARCHAR(20),
  c_business_info VARCHAR(100),
  c_passwd VARCHAR(20),
  c_contact_fname VARCHAR(15),
  c_contact_lname VARCHAR(15),
  c_addr VARCHAR(100),
  c_contact_phone VARCHAR(16),
  c_contact_email VARCHAR(50),
  c_payment_method VARCHAR(2),
  c_credit_info VARCHAR(300),
  c_discount FLOAT
);
CREATE TABLE item (
  i_id UUID PRIMARY KEY,
  i_title VARCHAR(60),
  i_pub_date TIMESTAMP,
  i_publisher VARCHAR(60),
  i_subject VARCHAR(60),
  i_desc VARCHAR(500),
  i_srp FLOAT,
  i_cost FLOAT,
  i_isbn CHAR(13),
  i_page INTEGER
);
CREATE TABLE orders (
  o_id UUID PRIMARY KEY,
  c_id UUID REFERENCES customer,
  o_date TIMESTAMP,
  o_sub_total FLOAT,
  o_tax FLOAT,
  o_total FLOAT,
  o_ship_type VARCHAR(10),
  o_ship_date TIMESTAMP,
  o_ship_addr VARCHAR(100),
  o_status VARCHAR(16)
);
CREATE TABLE order_line (
  ol_id UUID PRIMARY KEY,
  o_id UUID REFERENCES orders,
  i_id UUID REFERENCES item,
  ol_qty Integer,
  ol_discount FLOAT,
  ol_status VARCHAR(16)
);
