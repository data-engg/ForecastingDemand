set search_path to dwh;

DROP TABLE IF EXISTS s_dim_currency;

CREATE TEMPORARY TABLE s_dim_currency (like dim_currency);

ALTER TABLE s_dim_currency drop column latest_flag;
ALTER TABLE s_dim_currency drop column row_insrt_dttm;

copy s_dim_currency 
from 's3://anurag-sharma01/RetailCart/batch/retailcart_currency_details.txt'
iam_role :redshift_role
region 'ap-south-1'
format csv
DELIMITER '\t'
IGNOREHEADER 1;

BEGIN TRANSACTION;

update dim_currency
set latest_flag = FALSE 
from dim_currency dc
join s_dim_currency sdc on dc.currency_id = sdc.currency_id
where dc.latest_flag = TRUE ;

INSERT INTO dim_currency 
SELECT sdc.*
, True as latest_flag 
, sysdate as row_insrt_dttm 
from s_dim_currency sdc
order by sdc.currency_id;

END TRANSACTION;

