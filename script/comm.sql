
select T1.user_id, T2.imei, T1.sex as sex_idcard, T3.sex as sex_pred from algo.yf_age_gender_accord_idcard as T1 join user_profile.edl_device_uid_mz_rel as T2 on T1.user_id = T2.uid join algo.yf_sex_model_app_install_predict_on_30000dims as T3 on T2.imei = T3.imei where T2.stat_date=20170711 and T3.stat_date=20170711


