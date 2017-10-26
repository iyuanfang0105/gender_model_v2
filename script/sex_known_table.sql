create table if not exists algo.yf_imei_sex_for_build_sexmodel_flyme_new(imei string,sex int)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'
WITH SERDEPROPERTIES ('serialization.null.format'='')
STORED AS RCFILE;

insert overwrite table algo.yf_imei_sex_for_build_sexmodel_flyme_new select	s.imei, 1
from ( select user_id,sex from  user_center.mdl_flyme_users_info where stat_date= 20170711 and sex= 2) t
join (select imei,uid from  user_profile.edl_uid_all_info where stat_date= 20170711) s
on (t.user_id = s.uid);

insert into table algo.yf_imei_sex_for_build_sexmodel_flyme_new
select	s.imei, 0
from ( select user_id,sex from  user_center.mdl_flyme_users_info where stat_date= 20170711 and sex= 1) t
join (select imei,uid from  user_profile.edl_uid_all_info where stat_date= 20170711) s
on (t.user_id = s.uid);

select sex,count(*) from imei_sex_for_build_sexmodel_flyme_new group by sex
