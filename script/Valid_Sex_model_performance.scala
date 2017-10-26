import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.avg
import org.apache.spark.rdd.RDD


// Initial Hive
val hiveContext = new HiveContext(sc)
hiveContext.setConf("mapred.output.compress", "false")
hiveContext.setConf("hive.exec.compress.output", "false")
hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
println("================== Initial HIVE Done =========================")
import hiveContext.implicits._


// get timestamp
val calendar: Calendar = Calendar.getInstance()
val currentYear: Int = calendar.get(Calendar.YEAR)
val yestoday: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

val valid_day: String = "20170711"
//val selectSQL: String = "select T1.user_id, T2.imei, T1.sex as sex_idcard, T3.sex as sex_pred from algo.yf_age_gender_accord_idcard as T1 join user_profile.edl_device_uid_mz_rel as T2 on T1.user_id = T2.uid join algo.yf_sex_model_app_install_predict_on_30000_new_1 as T3 on T2.imei = T3.imei where T2.stat_date=" + valid_day + " and T3.stat_date=" + valid_day
val selectSQL: String = "select T1.imei, T1.sex as sex_idcard, T2.sex as sex_pred from algo.yf_sex_label_known_and_only_in_idcard as T1 join algo.yf_sex_model_app_install_predict_on_30000_new_1 as T2 on T1.imei = T2.imei where T2.stat_Date=" + valid_day
//val selectSQL: String = "select T1.imei, T1.sex as sex_idcard, T2.sex as sex_pred from algo.yf_sex_label_known_and_only_in_idcard as T1 join algo.sex_model_latest_predictionall as T2 on T1.imei = T2.imei"
val resDF: DataFrame = hiveContext.sql(selectSQL)
val validRes = resDF.select(avg(($"sex_idcard" === $"sex_pred").cast("integer")))
validRes.show()

