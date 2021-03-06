set search_path to dwh;

DROP TABLE IF EXISTS s_dim_department;

CREATE TEMPORARY TABLE s_dim_department (like dim_department);

ALTER TABLE s_dim_department drop column latest_flag;
ALTER TABLE s_dim_department drop column row_insrt_dttm;

copy s_dim_department 
from 's3://anurag-sharma01/RetailCart/batch/retailcart_department_details.txt'
iam_role :redshift_role
region 'ap-south-1'
format csv
DELIMITER '\t'
NULL AS 'NULL'
IGNOREHEADER 1;

BEGIN TRANSACTION;

update dim_department
set latest_flag = FALSE 
from dim_department dd
join s_dim_department sdd on dd.department_number = sdd.department_number
and dd.department_category_number = sdd.department_category_number
and dd.department_sub_category_number = sdd.department_sub_category_number
where dd.latest_flag = TRUE ;

INSERT INTO dim_department 
SELECT sdd.*
, True as latest_flag 
, sysdate as row_insrt_dttm 
from s_dim_department sdd
order by sdd.department_number, sdd.department_category_number, sdd.department_sub_category_number ;

END TRANSACTION;

