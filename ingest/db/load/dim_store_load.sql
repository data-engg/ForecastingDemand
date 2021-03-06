set search_path to dwh;

DROP TABLE IF EXISTS s_dim_store;

CREATE TEMPORARY TABLE s_dim_store (like dim_store);

ALTER TABLE s_dim_store drop column latest_flag;
ALTER TABLE s_dim_store drop column row_insrt_dttm;

copy s_dim_store 
from 's3://anurag-sharma01/RetailCart/batch/retailcart_store_details.txt'
iam_role :redshift_role
region 'ap-south-1'
format csv
DELIMITER '\t'
IGNOREHEADER 1;

BEGIN TRANSACTION;

update dim_store
set latest_flag = FALSE 
from dim_store ds
join s_dim_store sds on ds.store_id = sds.store_id
where ds.latest_flag = TRUE ;

INSERT INTO dim_store 
SELECT sds.*
, True as latest_flag 
, sysdate as row_insrt_dttm 
from s_dim_store sds
order by sds.store_id ;

END TRANSACTION;

