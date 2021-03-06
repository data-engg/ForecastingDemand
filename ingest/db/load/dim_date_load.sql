set search_path to dwh;

DROP TABLE IF EXISTS s_dim_date;

CREATE TEMPORARY TABLE s_dim_date (like dim_date);

ALTER TABLE s_dim_date drop column latest_flag;
ALTER TABLE s_dim_date drop column row_insrt_dttm;

copy s_dim_date 
from 's3://anurag-sharma01/RetailCart/batch/retailcart_calendar_details.txt'
iam_role :redshift_role
region 'ap-south-1'
format csv
DELIMITER '\t'
IGNOREHEADER 1;

BEGIN TRANSACTION;

update dim_date
set latest_flag = FALSE 
from dim_date dd
join s_dim_date sdd on dd.calendar_date = sdd.calendar_date
where dd.latest_flag = TRUE ;

INSERT INTO dim_date 
SELECT sdd.*
, True as latest_flag 
, sysdate as row_insrt_dttm 
from s_dim_date sdd
order by sdd.calendar_date;

END TRANSACTION;

