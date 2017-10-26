package SexModel.DataProcess

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object Prepare_data_set_for_build_sex_model {

  case class imei_sex(imei: String, sex: Int)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()

    System.setProperty("user.name", "mzsip")
    System.setProperty("HADOOP_USER_NAME", "mzsip")
    sparkConf.setAppName("YF_PREPARE_DATA_SET_FOR_BUILD_SEX_MODEL")

    val sc: SparkContext = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("mapred.output.compress", "false")

    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.setConf("mapred.output.compress", "false")
    hiveContext.setConf("hive.exec.compress.output", "false")
    hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
    import hiveContext.implicits._

    // get timestamp
    // val today = "20170711"
    val today = args(0) // mai
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month-1, day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val select_data_set_sql: String = "select imei, sex from " + "(select user_id,sex from user_center.mdl_flyme_users_info where stat_date= " + yestoday_Date + " and (sex= 2 OR sex=1)) t " + "join (select imei,uid from  user_profile.edl_uid_all_info where stat_date= " + yestoday_Date + " ) s on (t.user_id = s.uid)"
    val data_set_df: DataFrame = hiveContext.sql(select_data_set_sql)
    val data_set_rdd: RDD[(String, Int)] = data_set_df.map(v => {
      var sex: Int = -1
      if (v(1) == 2)
        sex = 1 // M
      else
        sex = 0 // F
      (v(0).toString, sex)
    }).reduceByKey((x, y) => x)
    val data_set_df_final: DataFrame = data_set_rdd.map(v => imei_sex(v._1, v._2)).toDF()


    val sex_count = data_set_rdd.map(_._2).countByValue()
    println("\n\n ******************** data_set_count_by_imei: " + data_set_df.count() +
      "  ************** data_set_count_uniq_by_imei: " + data_set_rdd.count() + "\n\n")
    println("\n\n ******************** data_set_M_count: " + sex_count(1) +
      "  ************** data_set_F_count: " + sex_count(0) + " ********* ratio: " + sex_count(1) * 1.0 / sex_count(0) + "\n\n")

    val data_set_table_name: String = "algo.yf_imei_sex_for_build_sexmodel_flyme"
    data_set_df_final.registerTempTable("temp")
    //val creatUserAgeLabelSQL: String = "create table if not exists " + userAgeLabelTable + " (user_id bigint, age_range string, sex string) partitioned by (stat_date string) stored as textfile"
    //val InsertUserAgeLabelSQL:String = "insert overwrite table " + userAgeLabelTable + " partition(stat_date = " + yestoday + ") select * from " + userAgeLabelTableTemp
    val creat_data_set_sql: String = "create table if not exists " + data_set_table_name + " (imei string, sex int) stored as textfile"
    val Insert_data_set_sql:String = "insert overwrite table " + data_set_table_name + " select * from temp"
    hiveContext.sql(creat_data_set_sql)
    hiveContext.sql(Insert_data_set_sql)
  }
}
