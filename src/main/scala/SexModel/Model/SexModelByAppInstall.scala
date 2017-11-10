package SexModel.Model

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yuanfang on 2017/08/16.
 */
object SexModelByAppInstall {

  case class Item(itemCode: String, itemName: String, itemColIndex: Long)
  case class Imei_feature(imei: String, features: String)
  case class Imei_sex(imei: String, sex: String)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()

    System.setProperty("user.name", "mzsip")
    System.setProperty("HADOOP_USER_NAME", "mzsip")
    sparkConf.setAppName("ALGO_YF_SEX_MODEL_BY_APP_INSTALL")

    val sc: SparkContext = new SparkContext(sparkConf)

    sc.hadoopConfiguration.set("mapred.output.compress", "false")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val hiveContext: HiveContext = new HiveContext(sc)
    hiveContext.setConf("mapred.output.compress", "false")
    hiveContext.setConf("hive.exec.compress.output", "false")
    hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")

    // get timestamp
    val today = "20171105"
    // val today = args(0) // mai
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year,month-1,day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

    val splitChar: String = "\u0001"

    // sex knonwn data
    val sex_know_table_name: String = "algo.yf_imei_sex_for_build_sexmodel_flyme"
    //app install topK
    val topK: Int = 30000
    // app install feature by imei
    val imei_feature_table_name: String = "algo.yf_sex_model_imei_app_install_features_on_" + topK.toString + "dims"


    // prepare data and train model
    val dim_num: Int = topK
    val num_classes: Int = 2
    val select_threshold_flag: Boolean = false
    val train_pred_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = getTrainSet_predictSet(hiveContext, sex_know_table_name, imei_feature_table_name, dim_num, splitChar, yestoday_Date)
    val model: LogisticRegressionModel = build_model(train_pred_set, num_classes, select_threshold_flag)

    // predict on data set of imei
    val pred: RDD[(String, String)] = predict(model, train_pred_set)
    val pred_male_count = pred.filter(_._2 == "male").count()
    val pred_female_count = pred.filter(_._2 == "female").count()
    println("\n\n ***************** The mumber of male in prediction: " + pred_male_count + " ************* \n\n")
    println("\n\n ***************** The mumber of female in prediction: " + pred_female_count + " ************* \n\n")
    println("\n\n ***************** The ratio of male vs female: " + pred_male_count * 1.0 / pred_female_count + " ************* \n\n")

    // write result to db
    // write_result_to_DB(hiveContext, pred, yestoday_Date)
  }

  def write_result_to_DB(hiveContext: HiveContext,
                         result: RDD[(String, String)],
                         yestoday_Date: String
                        ) = {
    // store the result
    import hiveContext.implicits._
    val pre_df: DataFrame = result.repartition(100).map(v => Imei_sex(v._1, v._2)).toDF
    pre_df.registerTempTable("prediction")

    println("\n\n ********************* (Strarting)Insert result to yf table *********************\n\n ")
    val create_predict_table_name: String = "algo.yf_sex_model_app_install_predict_on_30000_new_1"
    hiveContext.sql(
      "create table if not exists " +
        create_predict_table_name +
        " (imei string, sex string) partitioned by (stat_date string) stored as textfile")

    hiveContext.sql(
      "insert overwrite table " +
        create_predict_table_name +
        " partition(stat_date = " +
        yestoday_Date +
        " ) select * from prediction")
    println("\n\n ********************* (Done)Insert result to yf table *********************\n\n ")

    // insert the predict result to latest table, and another job will write the result
    // to final sex table
    println("\n\n ********************* (Strarting)Insert result to final table *********************\n\n ")
    val sex_last_pre_tableName: String = "algo.sex_model_latest_predictionAll"
    val create_sex_last_pre_tableSql: String =
      "create table if not exists " +
        sex_last_pre_tableName +
        " (imei string, sex string) stored as textfile"
    val insert_sex_last_pre_tableSql: String =
      "insert overwrite table " +
        sex_last_pre_tableName +
        " select * from prediction"
    hiveContext.sql(create_sex_last_pre_tableSql)
    hiveContext.sql(insert_sex_last_pre_tableSql)
    println("\n\n ********************* (Done)Insert result to final table *********************\n\n ")
  }


  def predict(model: LogisticRegressionModel, train_pre: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)])): RDD[(String, String)] = {
    val prediction: RDD[(String, String)] = train_pre._2.map(v => {
      if (model.predict(v._2.features) == 1d)
        (v._1, "male")
      else
        (v._1, "female")
    })
    val pre: RDD[(String, String)] = train_pre._1.map(v => (v._1, if (v._2.label == 1d) "male" else "female")).union(prediction)
    pre
  }


  def getTrainSet_predictSet(
                              hiveContext: HiveContext,
                              know_sex_table_name: String,
                              imei_feature_table_name: String,
                              dim_num: Int,
                              splitChar: String,
                              yestoday_Date: String): (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = {
    val select_know_sex_sql: String = "select * from " + know_sex_table_name
    //(imei, sex)
    val imei_sexLabel: RDD[(String, Double)] = hiveContext.sql(select_know_sex_sql).map(_.mkString(splitChar)).filter(_.split(splitChar).length == 2).map(v => {val array: Array[String] = v.trim.split(splitChar); (array(0).trim, array(1).trim.toDouble)})
    println("\n\n ********************* The number of imei known sex: " + imei_sexLabel.count() + " *********************** \n\n")

    val select_imei_feature_sql: String = "select * from " + imei_feature_table_name + " where stat_date=" + yestoday_Date
    val imei_feature_rdd: RDD[(String, String)] = hiveContext.sql(select_imei_feature_sql).rdd.map(v => (v(0).toString, v(1).toString))
    println("\n\n ********************* The number of imei: " + imei_feature_rdd.count() + " *********************** \n\n")

    //(imei, (sexLabel, nullFeature, sexKnowTag))
    val imei_sexLabel_tail: RDD[(String, (Double, String, Int))] = imei_sexLabel.map(v => (v._1, (v._2, "", 1)))
    //(imei, (nuxLabel, feature, sexUnKnowTag))
    val imei_features_tail: RDD[(String, (Double, String, Int))] = imei_feature_rdd.map(v => (v._1, (0d, v._2, 2)))

    val union_tail: RDD[(String, (Double, String, Int))] = imei_sexLabel_tail.union(imei_features_tail).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    //(imei, (sexLabel, feature, sexKnowTag))
    val imei_hasfeature_rdd: RDD[(String, (Double, String, Int))] = union_tail.filter(_._2._3 != 1)
    //(imei, (sex_label, index_arry[int], value_arry[double], sexKnowTag))
    val imei_hasfeature_rdd1: RDD[(String, (Double, Array[Int], Array[Double], Int))] = imei_hasfeature_rdd.map(v => {
      val features: Array[String] = v._2._2.trim.split(" ")
      val index_array: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      val value_array: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      for (feature <- features) {
        val columnIndex_value: Array[String] = feature.trim.split(":")
        if (columnIndex_value.length == 2) {
          index_array += columnIndex_value(0).trim.toInt
          value_array += columnIndex_value(1).trim.toDouble
        }
      }
      (v._1, (v._2._1, index_array.toArray, value_array.toArray, v._2._3))
    }).filter(_._2._2.length > 0)

    val predictSet: RDD[(String, LabeledPoint)] = imei_hasfeature_rdd1.filter(_._2._4 == 2).map(v => (v._1, new LabeledPoint(v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3))))
    // val predictSet: RDD[(String, Double, Vector)] = imei_hasfeature_rdd1.filter(_._2._4 == 2).map(v => (v._1, v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3)))
    // val predictSet_DF: DataFrame = hiveContext.createDataFrame(predictSet).toDF("imei", "label", "features")
    println("\n\n******************** The number of predictSet: " + predictSet.count + "  *********************\n\n")

    val trainSet: RDD[(String, LabeledPoint)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, new LabeledPoint(v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3))))
    // val trainSet: RDD[(String, Double, Vector)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3)))
    // val trainSet_DF: DataFrame = hiveContext.createDataFrame(trainSet).toDF("imei", "label", "features")
    println("\n\n******************** The number of trainSet: " + trainSet.count + "  *********************\n\n")
    (trainSet, predictSet)
  }


  def build_model(
                   data_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]),
                   classes_num: Int,
                   select_threshold_flag: Boolean
                 ): LogisticRegressionModel = {
    println("\n\n ********************* Build Model *************************")
    val trainSet: RDD[(String, LabeledPoint)] = data_set._1 // balance dataset

    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = trainSet.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()
    println("********************* Data set number: " + train_rdd.count() + " *************************")
    println("********************* label_0 count: " + train_rdd.filter(_._2.label == 0).count() + " *******************")
    println("********************* label_1 count: " + train_rdd.filter(_._2.label == 1).count() + " ******************* \n\n")

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))


    println("\n\n ****************** The Default threshold performance *************************** \n\n")
    val defalut_threshold: Double = model.getThreshold.get
    println("\n\n ************* The Default threshold: " + defalut_threshold + "***************** \n\n")
    val valid_result_default: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    report_model_performance(valid_result_default)


    println("\n\n ****************** The Best threshold performance *************************** \n\n")
    // choosing the best threshold
    model.clearThreshold()
    val valid_result_score: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val binaryClassificationMetrics = new BinaryClassificationMetrics(valid_result_score.map(_._2))
    val fMeasure = binaryClassificationMetrics.fMeasureByThreshold()
    val best_threshold = fMeasure.reduce((a, b) => {if(a._2 < b._2) b else a})
    model.setThreshold(best_threshold._1)
    println("\n\n ************* The Best threshold: " + model.getThreshold + "***************** \n\n")
    val valid_result_threshold: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    report_model_performance(valid_result_threshold)

    if (!select_threshold_flag)
      model.setThreshold(defalut_threshold)

    return model
  }

  def report_model_performance(valid_result: RDD[(String, (Double, Double))]): Unit ={
    val roc = ROC(valid_result.map(_._2))
    println("\n\n ********************** AUROC: " + roc._1 + " *********************")
    println("********************** Accuracy: " + roc._2 + " *********************")
  }

  def ROC(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)

    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")
    return (au_roc, accuracy)
  }

}
