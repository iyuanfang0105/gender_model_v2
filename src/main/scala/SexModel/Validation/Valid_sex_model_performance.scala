package SexModel.Validation

import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.sql.functions.avg
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object Valid_sex_model_performance {

  case class Accuracy(accuracy: Double, date: String)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()

    System.setProperty("user.name", "mzsip")
    System.setProperty("HADOOP_USER_NAME", "mzsip")
    sparkConf.setAppName("ALGO_YF_SEX_MODEL_V2_VALIDATION")

    val sc: SparkContext = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("mapred.output.compress", "false")

    // set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.setConf("mapred.output.compress", "false")
    hiveContext.setConf("hive.exec.compress.output", "false")
    hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
    import hiveContext.implicits._

    // get timestamp
    // val today = "20170917"
    val today = args(0)
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year,month-1,day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)


    // validation data set from idcard info which is not included by sex_model dataset
    val id_card_info_table: String = "algo.yf_age_gender_accord_idcard_v2"
    val predict_result_table: String = "algo.yf_sex_model_app_install_predict_on_30000_new_2"
    val select_latest_date_sql = "show PARTITIONS " + id_card_info_table
    val latest_date: String = hiveContext.sql(select_latest_date_sql).map(v => v(0).toString.split("=")(1).toInt).collect().sortWith((a, b) => a > b)(0).toString
    println("***************** The latest date of id_card_info_table: " + latest_date.toString() + " *************")

    val select_id_info_sql: String = "SELECT T1.imei, T1.gender as sex_idcard from " + id_card_info_table + " as T1 where stat_date=" + latest_date
    val select_predict_SQL: String = "select T2.imei, T2.sex as sex_pred from " + predict_result_table + " as T2"
    val id_card_info_df: DataFrame = hiveContext.sql(select_id_info_sql)
    val predict_res_df: DataFrame = hiveContext.sql(select_predict_SQL)

    val imei_matched = id_card_info_df.join(predict_res_df, id_card_info_df("imei") === predict_res_df("imei"))
    val correct_pred = imei_matched.rdd.filter(v => v(1).toString == v(3).toString)
    val accuracy = correct_pred.count() * 1.0 / imei_matched.count()

    // validation on dataset: yf_sex_label_known_and_only_in_idcard
    println("\n\n**************************** Starting evaluation *************************** \n\n")



    val valid_res = resDF.select(avg(($"sex_idcard" === $"sex_pred").cast("integer")))
    val accuracy: DataFrame = valid_res.map(v => Accuracy(v(0).toString.toDouble, yestoday_Date)).toDF()
    println("\n\n**************************** Accuracy is: ************************** \n\n")
    accuracy.show()


    // validation on dataset: yf_sex_label_known_and_only_in_idcard
    println("\n\n**************************** Starting evaluation *************************** \n\n")
    val selectSQL: String = "select T1.imei, T1.sex as sex_idcard, T2.sex as sex_pred from algo.yf_sex_label_known_and_only_in_idcard as T1 join algo.sex_model_latest_predictionall as T2 on T1.imei = T2.imei"
    val resDF: DataFrame = hiveContext.sql(selectSQL)
    val valid_res = resDF.select(avg(($"sex_idcard" === $"sex_pred").cast("integer")))
    val accuracy: DataFrame = valid_res.map(v => Accuracy(v(0).toString.toDouble, yestoday_Date)).toDF()
    println("\n\n**************************** Accuracy is: ************************** \n\n")
    accuracy.show()

    val valid_res_1 = resDF.rdd.filter(v => v(1).toString == v(2).toString).count() * 1.0 / resDF.count()


    // write the result to table algo.yf_sex_model_performance
    accuracy.registerTempTable("accuracy_temp_table")
    println("\n\n**************************** Starting writing table *************************** \n\n")
    val create_table_name: String = "algo.yf_sex_model_performance_new"
    val create_table_sql: String = "create table if not exists " +
                                  create_table_name +
                                  " (accuracy double, date string) stored as textfile"
    val insert_table_sql: String =
      "insert overwrite table " +
        create_table_name +
        " select * from accuracy_temp_table"

    hiveContext.sql(create_table_sql)
    hiveContext.sql(insert_table_sql)
    println("\n\n**************************** Finished writing table *************************** \n\n")
  }
}
