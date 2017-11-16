package SexModel.Validation

import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object Valid_sex_model_performance {

  case class Accuracy(date: String, accuracy_full: Double, accuracy_part: Double)

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
    val today = "20171105"
    // val today = args(0)
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year,month-1,day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)


    // validation data set from idcard info which is not included by sex_model dataset
    val id_card_info_full_table: String = "algo.yf_age_gender_accord_idcard_v2"
    val id_card_info_part_table: String = "algo.yf_sex_label_known_and_only_in_idcard"
    // val predict_result_table: String = "algo.yf_sex_model_app_install_predict_on_30000_new_1"
    val predict_result_table: String = "algo.sex_model_latest_predictionall"

    val select_latest_date_sql = "show PARTITIONS " + id_card_info_full_table
    val latest_date: String = hiveContext.sql(select_latest_date_sql).map(v => v(0).toString.split("=")(1).toInt).collect().sortWith((a, b) => a > b)(0).toString
    println("***************** The latest date of id_card_info_full_table: " + latest_date.toString() + " *************")

    val select_id_info_full_sql: String = "SELECT imei, gender as sex_idcard from " + id_card_info_full_table + " where stat_date=" + latest_date
    val select_id_info_part_sql: String = "select imei, sex as sex_idcard from " + id_card_info_part_table
    val select_predict_res_sql: String = "select imei, sex as sex_pred from " + predict_result_table
    val id_card_info_full_df: DataFrame = hiveContext.sql(select_id_info_full_sql)
    val id_card_info_part_df: DataFrame = hiveContext.sql(select_id_info_part_sql)
    val predict_res_df: DataFrame = hiveContext.sql(select_predict_res_sql)

    // evaluation using full data of id card
    val imei_matched_0 = id_card_info_full_df.join(predict_res_df, id_card_info_full_df("imei") === predict_res_df("imei"))
    val correct_pred_0 = imei_matched_0.rdd.filter(v => v(1).toString == v(3).toString)
    val accuracy_0 = correct_pred_0.count() * 1.0 / imei_matched_0.count()
    println("\n\n***************** Evaluation using full data of id card (accuracy):" + accuracy_0  + "************ \n\n")

    // evaluation using part data of id card
    val imei_matched_1 = id_card_info_part_df.join(predict_res_df, id_card_info_part_df("imei") === predict_res_df("imei"))
    val correct_pred_1 = imei_matched_1.rdd.filter(v => v(1).toString == v(3).toString)
    val accuracy_1 = correct_pred_1.count() * 1.0 / imei_matched_1.count()
    println("\n\n***************** Evaluation using part data of id card (accuracy):" + accuracy_1  + "************ \n\n")


    // write the result to table algo.yf_sex_model_performance
    val accuracy: DataFrame = Seq((accuracy_0, accuracy_1)).map(v => Accuracy(yestoday_Date, v._1, v._2)).toDF()
    accuracy.registerTempTable("accuracy_temp_table")
    println("\n\n**************************** Starting writing table *************************** \n\n")
    val performance_table_name: String = "algo.yf_sex_model_performance_new"
    val create_performance_table_sql: String = "create table if not exists " + performance_table_name +
                                  " (date string, accuracy_full double, accuracy_part double) stored as textfile"
    val insert_table_sql: String = "insert into table " + performance_table_name + " select * from accuracy_temp_table"
    hiveContext.sql(create_performance_table_sql)
    hiveContext.sql(insert_table_sql)
    println("\n\n**************************** Finished writing table *************************** \n\n")

    // compare_models_performance(sc, hiveContext)
  }


  def compare_models_performance(sc: SparkContext,
                                 hiveContext: HiveContext) = {
    // user behavior of app install
    val splitChar: String = "\u0001"
    val source_feature_table_name: String = "app_center.adl_fdt_app_adv_model_install"
    // (imei, appid)
    val user_app_0: RDD[(String, String)] = get_app_user(hiveContext, source_feature_table_name, splitChar, "20171028")
    val user_app_1: RDD[(String, String)] = get_app_user(hiveContext, source_feature_table_name, splitChar, "20170910")

    val select_data_0 = "SELECT imei as imei_v3, sex as sex_v3 from algo.yf_sex_model_app_install_predict_on_30000_new_1 where stat_date=20171028"
    val select_data_1 = "SELECT imei as imei_v2, sex as sex_v2 from algo.yf_sex_model_app_install_predict_on_30000_new_1 where stat_date=20170910"

    val data_0: DataFrame = hiveContext.sql(select_data_0)
    val data_1: DataFrame = hiveContext.sql(select_data_1)

    val data = data_0.join(data_1, data_0("imei_v3") === data_1("imei_v2")).select("imei_v3", "sex_v3", "sex_v2")
    // imei, sex_0, sex_1
    val data_dismatch: RDD[(String, (String, String))] = data.rdd.repartition(100).map(v => (v.getString(0), (v.getString(1), v.getString(2)))).filter(v => v._2._1 != v._2._2)

    // val female_apps = sc.parallelize(Seq(("2338089", "female"), ("2489797", "female")))
    val female_apps = sc.parallelize(Seq(("2338089", "female")))
    // user_app (imei, app_id)
    // res (app_id, (sex_0, sex_1)) join (app_id, " ")
    val data_dismatch_v3_female = data_dismatch.filter(_._2._1 == "female")
    val data_dismatch_v3_male = data_dismatch.filter(_._2._1 == "male")

    val data_dismatch_v2_female = data_dismatch.filter(_._2._2 == "female")
    val data_dismatch_v2_male = data_dismatch.filter(_._2._2 == "male")

    val res_v3 = data_dismatch_v3_female.join(user_app_0).map(v => (v._2._2, (v._2._1._1, v._2._1._2))).join(female_apps)
    val res_v2 = data_dismatch_v2_female.join(user_app_1).map(v => (v._2._2, (v._2._1._1, v._2._1._2))).join(female_apps)

    val res_v3_accuracy = res_v3.count() * 1.0 / data_dismatch_v3_female.count()
    val res_v2_accuracy = res_v2.count() * 1.0 / data_dismatch_v2_female.count()
    println("\n\n ************************** res_v3: " + res_v3.count() + "*********************\n\n")
    println("\n\n ************************** res_v2: " + res_v2.count() + "*********************\n\n")
    println("\n\n ************************** res_v3_accuracy: " + res_v3_accuracy + "*********************\n\n")
    println("\n\n ************************** res_v2_accuracy: " + res_v2_accuracy + "*********************\n\n")

  }

  // get the user behavior of install app, format: (imei, appid)
  def get_app_user(
                    hiveContext: HiveContext,
                    source_feature_table_name: String,
                    splitChar: String,
                    yestoday_Date: String): RDD[(String, String)] = {
    val select_source_feature_table_sql: String = "select * from " + source_feature_table_name + " where stat_date = " + yestoday_Date
    //(imei, feature)
    val imei_features_df: DataFrame = hiveContext.sql(select_source_feature_table_sql)
    //println("count of imei_features_df for " + sqls_dataType(i)._2 + ": " + imei_features_df.count())
    val imei_features_rdd1: RDD[(String, Array[String])] = imei_features_df.map(v => (v(0).toString, v(1).toString.trim.split(" ")))
    val imei_features_rdd2 = imei_features_rdd1.filter(_._2.length > 0)
    //println("count of imei_features_rdd2 for " + sqls_dataType(i)._2 + ": " + imei_features_rdd2.count)
    val imei_features_rdd3 = imei_features_rdd2.mapPartitions(iter => {
      new Iterator[(String, String)]() {
        var count: Int = 0
        var value: (String, Array[String]) = iter.next()
        override def hasNext: Boolean = {
          if (count < value._2.length)
            true
          else {
            if (iter.hasNext) {
              count = 0
              value = iter.next()
              true
            }
            else
              false
          }
        }

        override def next(): (String, String) = {
          count += 1
          (value._2(count - 1), value._1)
        }
      }
    })
    //println("count of imei_features_rdd3 for " + sqls_dataType(i)._2 + ": " +  + imei_features_rdd3.count())
    val imei_features_rdd4 = imei_features_rdd3.filter(_._1.trim.split(":").length == 2).map(v => {
      val array: Array[String] = v._1.trim.split(":")
      (v._2, array(0).trim.substring(2))
    })
    //println("count of imei_features_rdd4 for" + sqls_dataType(i)._2 + ": " + + imei_features_rdd4.count())
    imei_features_rdd4
  }

}


