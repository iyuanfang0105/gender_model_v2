package SexModel.DataProcess

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yuanfang on 2017/8/15.
  */
object AppInstallDataProcess {
  case class Item(itemCode: String, itemName: String, itemColIndex: Long)
  case class Imei_feature(imei: String, features: String)
  case class Imei_sex(imei: String, sex: String)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()

    System.setProperty("user.name", "mzsip")
    System.setProperty("HADOOP_USER_NAME", "mzsip")
    sparkConf.setAppName("YF_ALGO_sex_mode_appInstall_actionBased")

    val sc: SparkContext = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("mapred.output.compress", "false")

    // val today = "20170711"
    val today = args(0)
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year,month-1,day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val splitChar: String = "\u0001"
    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.setConf("mapred.output.compress", "false")
    hiveContext.setConf("hive.exec.compress.output", "false")
    hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")

    //原始维表的表名称
    val source_dim_table_name: String = "app_center.adl_sdt_adv_dim_app"
    //原始的用户行为表的名称
    val source_feature_table_name: String = "app_center.adl_fdt_app_adv_model_install"
    //筛选出安装量最大的topK个item
    val topK: Int = 30000
    //item的code前缀
    val code_prefix: String = "11"
    //将要创建的新的维表名称
    val create_dim_table_name: String = "algo.yf_sex_model_app_install_items_on_" + topK.toString + "dims"
    //将要创建的新的用户行为表名称
    val create_feature_table_name: String = "algo.yf_sex_model_imei_app_install_features_on_" + topK.toString + "dims"

    //(itemCode, (itemName, installNum))
    //(code_prefix+appid, (appname, installnum))
    val source_topK_rdd: RDD[(String, (String, Long))] = get_topK_rdd(hiveContext, topK, code_prefix, source_dim_table_name, splitChar, yestoday_Date)
    //(itemCode, (itemName, value))
    //(code_prefix+appid, (imei, feature))
    val source_feature_rdd: RDD[(String, (String, Double))] = get_features_rdd(hiveContext, source_feature_table_name, splitChar, yestoday_Date)
    println(" \n\n**************  count of source_dim_rdd: " + source_topK_rdd.count() + "  *********************")
    println(" **************  count of source_feature_rdd: " + source_feature_rdd.count() + "  *********************\n\n")

    val use_topK_rdd: RDD[(String, (String, Long))] = get_use_item_rdd(hiveContext, create_dim_table_name, source_feature_rdd, source_topK_rdd, yestoday_Date)
    // imei columnIndex1:value1 columnIndex2:value2 ...
    val imei_feature_rdd: RDD[(String, String)] = get_user_feature_data(hiveContext, create_feature_table_name, source_feature_rdd, use_topK_rdd, yestoday_Date)
    println(" \n\n**************  count of use_topK_rdd: " + use_topK_rdd.count() + "  *********************")
    println(" **************  count of imei_feature_rdd: " + imei_feature_rdd.count() + "  *********************\n\n")

  }

  /**
    * 根据真正使用的app和出现在行为表中的app，关联得到用户的有效行为数据，并写入hive表中
    * @param hiveContext                    用于从hive中读取和写入数据
    * @param create_feature_table_name      将要创建的新的用户行为表名称
    * @param source_feature_rdd             用户的行为数据，格式为：RDD[(itemCode, (itemName, value))]
    * @param use_item_rdd                   真正使用到的app，格式为：RDD[(itemCode, (itemName, columnIndex))]
    * @param yestoday_Date                  数据日期
    * 写入hive表中的用户新的行为数据格式为：imei columnIndex1:value1 columnIndex2:value2 ...
    */
  def get_user_feature_data(
                             hiveContext: HiveContext,
                             create_feature_table_name: String,
                             source_feature_rdd: RDD[(String, (String, Double))],
                             use_item_rdd: RDD[(String, (String, Long))],
                             yestoday_Date: String): RDD[(String, String)] = {
    val create_feature_table_sql: String =
      "create table if not exists " +
        create_feature_table_name +
        " (imei string, feature string) partitioned by (stat_date bigint) stored as textfile"
    val insertInto_feature_table_sql: String = "insert overwrite table " +
      create_feature_table_name +
      " partition(stat_date = " + yestoday_Date  + ") select * from "

    //(imei, columnIndex:value)             //(imei, columnIndex:value)            (code, (imei, value))  join  (code, (itemName, columnIndex))
    val join_rdd: RDD[(String, Array[(Long, String)])] = source_feature_rdd.join(use_item_rdd).map(v => (v._2._1._1, Array((v._2._2._2, "1"))))

    val imei_feature: RDD[(String, String)] = join_rdd.coalesce(200,true).reduceByKey(_ ++ _).map(v => {
      val array: Array[(Long, String)] = v._2.sortWith((a, b) => a._1 < b._1)
      val feature_str: String = array.map(v => v._1.toString + ":" + v._2).mkString(" ")
      (v._1, feature_str)
    })
    println(" **************** Function(get_user_feature_data): count of imei_feature " + imei_feature.count() + "  *******************")
    println(" **************** Function(get_user_feature_data): start insert table *******************")
    import hiveContext.implicits._
    val imei_feature_df: DataFrame = imei_feature.map(v => Imei_feature(v._1, v._2)).toDF()
    val tmp_table_name: String = "imei_feature_table"
    imei_feature_df.registerTempTable(tmp_table_name)
    hiveContext.sql(create_feature_table_sql)
    hiveContext.sql(insertInto_feature_table_sql + tmp_table_name)
    println(" **************** Function(get_user_feature_data): finished insert table *******************")
    imei_feature
  }

  /**
    * 根据筛选出来的app和出现在行为表里面的app的code进行关联，得到将要使用的app，并将得到的结果写入维度表中
    * @param hiveContext                    用于从hive读取数据和写入数据
    * @param create_dim_table_name          将要创建的新的维度表名称
    * @param source_feature_rdd             用户的行为数据，格式为：RDD[(itemCode, (itemName, value))]
    * @param source_dim_rdd                 筛选出来的app，格式为：RDD[(itemCode, itemName))
    * @param yestoday_Date                  数据日期
    * @return                               RDD[(itemCode, (itemName, columnIndex))]
    */
  def get_use_item_rdd(
                        hiveContext: HiveContext,
                        create_dim_table_name: String,
                        source_feature_rdd: RDD[(String, (String, Double))],
                        source_dim_rdd: RDD[(String, (String, Long))],
                        yestoday_Date: String): RDD[(String, (String, Long))] = {
    val create_dim_table_sql: String =
      "create table if not exists " +
        create_dim_table_name +
        " (code string, name string, column_index bigint) partitioned by (stat_date bigint) stored as textfile"
    val insertInto_dim_table_sql: String = "insert overwrite table " +
      create_dim_table_name +
      " partition(stat_date = " + yestoday_Date  + ") select * from "

    val code_rdd: RDD[(String, Int)] = source_feature_rdd.map(_._1).distinct().map(v => (v.trim, 1))

    println(" \n\n*************** Function(get_use_item_rdd): count of code_rdd: " + code_rdd.count() + "  *****************")

    val join_rdd: RDD[(String, (String, Long))] = source_dim_rdd.join(code_rdd).map(v => (v._1, v._2._1))

    //(code, (itemName, columnIndex))
    val use_items: RDD[(String, (String, Long))] = join_rdd.zipWithIndex().map(v => (v._1._1, (v._1._2._1, v._2)))
    println("  ****************** Function(get_use_item_rdd): count of use_items: " + use_items.count() + "*******************\n\n")

    println(" **************** Function(get_use_item_rdd): start insert table *******************")
    import hiveContext.implicits._
    val use_items_df: DataFrame = use_items.map(v => Item(if (v._1.length > 2) v._1.substring(2, v._1.length) else v._1, v._2._1, v._2._2)).toDF()
    val tmp_table_name: String = "item_table"
    use_items_df.registerTempTable(tmp_table_name)
    hiveContext.sql(create_dim_table_sql)
    hiveContext.sql(insertInto_dim_table_sql + tmp_table_name)
    println(" **************** Function(get_use_item_rdd): finish insert table *******************")
    use_items
  }

  /**
    * 将用户的行为记录转换成：（code, (imei, value))的格式
    * @param hiveContext                    用于从hive中读取item的维度表
    * @param source_feature_table_name      原始用户行为表
    * @param splitChar                      用于分隔字段的临时分隔符
    * @param yestoday_Date                  数据日期
    * @return                               RDD[(itemCode, (itemName, value))]
    * @return                               RDD[(appid, (imei, feature))]
    */
  def get_features_rdd(
                        hiveContext: HiveContext,
                        source_feature_table_name: String,
                        splitChar: String,
                        yestoday_Date: String): RDD[(String, (String, Double))] = {
    val select_source_feature_table_sql: String = "select * from " +
      source_feature_table_name +
      " where stat_date = " +
      yestoday_Date
    //(imei, feature)
    val imei_features_df: DataFrame = hiveContext.sql(select_source_feature_table_sql)
    //println("count of imei_features_df for " + sqls_dataType(i)._2 + ": " + imei_features_df.count())
    val imei_features_rdd1 = imei_features_df
      .map(v => v.mkString(splitChar))
      .map(v => {
        val array: Array[String] = v.trim.split(splitChar)
        (array(0).trim, array(1).trim)
      }).map(v => (v._1, v._2.trim.split(" ")))
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
    val imei_features_rdd4 = imei_features_rdd3.filter(_._1.trim.split(":").length == 2)
      .map(v => {
        val array: Array[String] = v._1.trim.split(":")
        (array(0).trim, (v._2, array(1).trim.toDouble))
      })
    //println("count of imei_features_rdd4 for" + sqls_dataType(i)._2 + ": " + + imei_features_rdd4.count())
    imei_features_rdd4
  }

  /**
    * 读取各个类型数据的维度表，将itemID转换成code,后面会用code作为key进行关联
    * @param hiveContext                  用于从hive中读取item的维度表
    * @param topK                         筛选出安装量最大的topK个app
    * @param code_prefix                  添加的code前缀
    * @param source_dim_table_name        原始维表
    * @param splitChar                    用于分隔字段的临时分隔符
    * @param yestoday_Date                数据日期
    * @return                             RDD[(itemCode, (itemName, installNum))]
    * @return                             RDD[("11"+appid, (app_name, installnum))]
    */
  def get_topK_rdd(
                    hiveContext: HiveContext,
                    topK: Int,
                    code_prefix: String,
                    source_dim_table_name: String,
                    splitChar: String,
                    yestoday_Date: String): RDD[(String, (String, Long))] = {
    val select_source_dim_table_sql: String = "select * from " +
      source_dim_table_name +
      " where stat_date = " +
      yestoday_Date
    //(appid, app_name, installnum)
    val item_df: DataFrame = hiveContext.sql(select_source_dim_table_sql)
    //(itemCode, (itemName, installNum))
    //("11"+appid, (app_name, installnum))
    val rdd: RDD[(String, (String, Long))] = item_df.map(v => v.mkString(splitChar).replace("\n", "")).map(v => { val array: Array[String] = v.trim.split(splitChar); (code_prefix.trim + array(0).trim, (array(1).trim, array(2).trim.toLong))})
    //(installNum, (itemCode, itemName))
    val topK_array: Array[(Long, (String, String))] = rdd.map(v => (v._2._2, (v._1, v._2._1))).top(topK)
    //(itemCode, (itemName, installNum))
    hiveContext.sparkContext.parallelize(topK_array).map(v => (v._2._1, (v._2._2, v._1)))
  }
}
