set search_path to dwh;

DROP TABLE IF EXISTS s_dim_item;

CREATE TEMPORARY TABLE s_dim_item (like dim_item);

ALTER TABLE s_dim_item drop column latest_flag;
ALTER TABLE s_dim_item drop column row_insrt_dttm;
ALTER TABLE s_dim_item drop column department_number;
ALTER TABLE s_dim_item drop column department_category_number;
ALTER TABLE s_dim_item drop column department_sub_category_number;
ALTER TABLE s_dim_item add column department_number varchar(50);
ALTER TABLE s_dim_item add column department_category_number varchar(50);
ALTER TABLE s_dim_item add column department_sub_category_number varchar(50);

copy s_dim_item 
(item_id, geo_region_cd, item_description, unique_product_cd, unique_product_cd_desc, department_number, 
department_category_number, department_sub_category_number, vendor_name, vendor_number, item_status_cd, item_status_desc, unit_cost)
from 's3://anurag-sharma01/RetailCart/batch/retailcart_item_details.txt'
iam_role :redshift_role
region 'ap-south-1'
DELIMITER '\t'
NULL AS 'NULL'
IGNOREHEADER 1;

UPDATE dim_item 
set latest_flag = FALSE 
from dim_item di
inner join s_dim_item sdi on di.item_id = sdi.item_id
where di.latest_flag = TRUE;

INSERT INTO dim_item 
select sdi.item_id,
sdi.geo_region_cd,
sdi.item_description,
sdi.unique_product_cd,
sdi.unique_product_cd_desc,
cast (split_part(department_number, ':',2) as smallint) as department_number,
cast (split_part(department_category_number, ':', 2) as int) as department_category_number,
cast (split_part(department_sub_category_number, ':', 2) as int) as department_sub_category_number, 
vendor_name,
vendor_number,
item_status_cd,
item_status_desc,
case when unit_cost is null then 0.0 else unit_cost end as unit_cost,
true as latest_flag,
sysdate as row_insrt_dttm
from s_dim_item sdi
order by item_id;

