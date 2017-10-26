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
val select_idcard_sql: String = "select T1.user_id, T2.imei, T1.sex as sex_idcard from algo.yf_age_gender_accord_idcard as T1 join user_profile.edl_device_uid_mz_rel as T2 on T1.user_id = T2.uid where T2.stat_date=" + valid_day
val select_flyme_sql: String = "select T.imei, T.sex as sex_flyme from algo.yf_imei_sex_for_build_sexmodel_flyme_new as T"

val idcard_df: DataFrame = hiveContext.sql(select_idcard_sql).select("imei", "sex_idcard")
//val idcard_uniq_by_imei_rdd: RDD[(String, String)] = idcard_df.map(v => (v(0).toString, v(1).toString)).reduceByKey((x, y) => x)
val idcard_uniq_by_imei_rdd: RDD[(String, Int)] = idcard_df.map(v => {
  var sex_idcard_int: Int = -1
  if (v(1) == "male")
    sex_idcard_int = 1
  else
    sex_idcard_int = 0
  (v(0).toString, sex_idcard_int)
}).reduceByKey((x, y) => x)

val idcard_uniq_by_imei_rdd_count: Double = idcard_uniq_by_imei_rdd.count()

val flyme_df: DataFrame = hiveContext.sql(select_flyme_sql)
val flyme_uniq_by_imei_rdd: RDD[(String, Int)] = flyme_df.map(v => (v(0).toString, v(1).toString.toInt)).reduceByKey((x, y) => x)
val flyme_uniq_by_imei_rdd_count: Double = flyme_uniq_by_imei_rdd.count()

val idcard_flyme_join_by_same_imei_rdd = idcard_uniq_by_imei_rdd.join(flyme_uniq_by_imei_rdd)
val idcard_flyme_join_by_same_imei_rdd_count = idcard_flyme_join_by_same_imei_rdd.count()
//val idcard_flyme_join_by_same_imei_df: DataFrame = idcard_df.join(flyme_df, idcard_df("imei") === flyme_df("imei"))
val only_in_idcard_rdd: RDD[(String, Int)] = idcard_uniq_by_imei_rdd.subtractByKey(flyme_uniq_by_imei_rdd)
val only_in_idcard_rdd_count: Double = only_in_idcard_rdd.count()
val only_in_flyme_rdd: RDD[(String, Int)] = flyme_uniq_by_imei_rdd.subtractByKey(idcard_uniq_by_imei_rdd)
val only_in_flyme_rdd_count: Double = only_in_flyme_rdd.count()

val idcard_flyme_join_by_same_imei_matches_rdd: RDD[(String, (Int, Int))] = idcard_flyme_join_by_same_imei_rdd.filter(v => v._2._1 == v._2._2)
val idcard_flyme_join_by_same_imei_matches_rdd_count: Double = idcard_flyme_join_by_same_imei_matches_rdd.count()
val idcard_flyme_join_by_same_imei_dismatches_rdd: RDD[(String, (Int, Int))] = idcard_flyme_join_by_same_imei_rdd.filter(v => v._2._1 != v._2._2)
val idcard_flyme_join_by_same_imei_dismatches_rdd_count: Double = idcard_flyme_join_by_same_imei_dismatches_rdd.count()
//val valid_res_null = resDF_temp.filter(v => v._3 == "null").count()


println("\n\n=================== valid flyme sex label against idcard sex label")
println("======>>>> idcard_flyme_join_by_same_imei_rdd_count: " + idcard_flyme_join_by_same_imei_rdd_count)
println("======>>>> idcard_flyme_join_by_same_imei_dismatches_rdd_count " + idcard_flyme_join_by_same_imei_dismatches_rdd_count + " ratio: " + idcard_flyme_join_by_same_imei_dismatches_rdd_count * 1.0 / idcard_flyme_join_by_same_imei_rdd_count)
println("======>>>> idcard_flyme_join_by_same_imei_matches_rdd_count " + idcard_flyme_join_by_same_imei_matches_rdd_count + " ratio: " + idcard_flyme_join_by_same_imei_matches_rdd_count * 1.0 / idcard_flyme_join_by_same_imei_rdd_count)

println("======>>>> idcard total(uniq by imei): " + idcard_uniq_by_imei_rdd_count)
println("======>>>> join with flyme by imei: " + idcard_flyme_join_by_same_imei_rdd_count)
println("======>>>> only in idcard: " + only_in_idcard_rdd_count)
println("======>>>> only in flyme: " + only_in_flyme_rdd_count + "\n\n")


// insert idcard_flyme_join_by_same_imei_rdd to table alog.yf_imei_sex_for_build_sexmodel_flyme_new
case class Imei_sex(imei: String, sex: Int)
val idcard_flyme_join_by_same_imei_matches_df: DataFrame = idcard_flyme_join_by_same_imei_matches_rdd.map(v => Imei_sex(v._1, v._2._1)).toDF()
val insert_idcard_for_train_sql = "insert into table algo.yf_imei_sex_for_build_sexmodel_flyme_new select * from temp_table"
idcard_flyme_join_by_same_imei_matches_df.registerTempTable("temp_table")
hiveContext.sql(insert_idcard_for_train_sql)


// insert only_in_idcard_rdd to table alog.yf_sex_known_data_only_in_idcard
case class Imei_sex_1(imei: String, sex: String)
val only_in_idcard_df: DataFrame = only_in_idcard_rdd.map(v => {
  var sex_str: String = ""
  if (v._2 == 1)
    sex_str = "male"
  else
    sex_str = "female"
  (v._1, sex_str)
}).map(v => Imei_sex_1(v._1, v._2)).toDF()


val only_in_idcard_df_insert_sql: String = "insert overwrite table algo.yf_sex_label_known_and_only_in_idcard select * from temp_table_1"
only_in_idcard_df.registerTempTable("temp_table_1")
hiveContext.sql(only_in_idcard_df_insert_sql)


