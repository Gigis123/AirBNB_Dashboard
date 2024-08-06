'''
=================================================
Milestone 3

Nama  : Achmad Abdillah Ghifari
Batch : FTDS-006-BSD

This program is made to create a table in sql or pgadmin called table_m3 and then copying the data from a csv to the newly created table. this is done for setting up the workflow using apache airflow where we fetch the data from sql  

The dataset is taken from kaggle from the following link = https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata/data
=================================================
'''

-- creating a begin to have a checkpoint
begin;

-- creating the table
CREATE TABLE table_m3 (
	"id" bigint, 
	"NAME" varchar(255),
	"host id" bigint, 
	"host_identity_verified" varchar(255), 
	"host name" varchar(255),
    "neighbourhood group" varchar(255), 
	"neighbourhood" varchar(255), 
	"lat" float, 
	"long" float, 
	"country" varchar(255),
    "country code" varchar(255), 
	"instant_bookable" varchar(255), 
	"cancellation_policy" varchar(255), 
	"room type" varchar(255),
    "Construction year" float, 
	"price" varchar(255), 
	"service fee" varchar(255), 
	"minimum nights" float,
    "number of reviews" float, 
	"last review" varchar(255), 
	"reviews per month" float,
    "review rate number" float, 
	"calculated host listings count" float,
    "availability 365" float, 
	"house_rules" varchar(10000),
	"license" varchar(255)
);

-- creating a rollback function if there is a mistake
ROLLBACK
-- creating a commit function if the change is accepted
COMMIT
-- dropping table
DROP TABLE table_m3
-- make a copy for the table_m3
copy table_m3
from 'C:\Users\THINKPAD X1 CAROBON\Hacktiv8\Phase 2\Week 2\Day 3\P2M3_A.A.Ghifari_data_raw.csv'
delimiter ','
csv header ;
-- show table m3
select * from table_m3
