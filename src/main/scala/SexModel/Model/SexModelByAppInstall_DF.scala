package SexModel.Model

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.max

import scala.collection.mutable.ArrayBuffer

/**
 * Created by yuanfang on 2017/08/16.
 */
object SexModelByAppInstall_DF {

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
    val create_predict_table_name: String = "algo.yf_sex_model_app_install_predict_on_" + topK.toString + "_new_1"

    // prepare data and train model
    val dim_num: Int = topK
    val train_pred_set: (DataFrame, DataFrame) = getTrainSet_predictSet(hiveContext, sex_know_table_name, imei_feature_table_name, dim_num, splitChar, yestoday_Date)
    val model: LogisticRegressionModel = build_model_based_df(train_pred_set)
    val valid_result = predict(model, train_pred_set._2)
    val auc_accu = report_performance(valid_result)
    println("\n\n ********************** AUROC: " + auc_accu._1 + " *********************")
    println("********************** Accuracy: " + auc_accu._2 + " *********************")

//    // predict on data set of imei
//    val pred: RDD[(String, String)] = predict(model._1, train_pred_set)
//    // store the result
//    import hiveContext.implicits._
//    val pre_df: DataFrame = pred.repartition(100).map(v => Imei_sex(v._1, v._2)).toDF
//    println("\n\n ********************* (Strarting)Insert result to yf table *********************\n\n ")
//    pre_df.registerTempTable("prediction")
//    hiveContext.sql(
//      "create table if not exists " +
//        create_predict_table_name +
//        " (imei string, sex string) partitioned by (stat_date string) stored as textfile")
//    hiveContext.sql(
//      "insert overwrite table " +
//        create_predict_table_name +
//        " partition(stat_date = " +
//        yestoday_Date +
//        " ) select * from prediction")
//    println("\n\n ********************* (Done)Insert result to yf table *********************\n\n ")
//
//    // insert the predict result to latest table, and another job will write the result
//    // to final sex table
//    println("\n\n ********************* (Strarting)Insert result to final table *********************\n\n ")
//    val sex_last_pre_tableName: String = "algo.sex_model_latest_predictionAll"
//    val create_sex_last_pre_tableSql: String =
//      "create table if not exists " +
//        sex_last_pre_tableName +
//        " (imei string, sex string) stored as textfile"
//    val insert_sex_last_pre_tableSql: String =
//      "insert overwrite table " +
//        sex_last_pre_tableName +
//        " select * from prediction"
//    hiveContext.sql(create_sex_last_pre_tableSql)
//    hiveContext.sql(insert_sex_last_pre_tableSql)
//    println("\n\n ********************* (Done)Insert result to final table *********************\n\n ")
  }

  def getTrainSet_predictSet(
                              hiveContext: HiveContext,
                              know_sex_table_name: String,
                              imei_feature_table_name: String,
                              dim_num: Int,
                              splitChar: String,
                              yestoday_Date: String): (DataFrame, DataFrame) = {
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

    // val predictSet: RDD[(String, LabeledPoint)] = imei_hasfeature_rdd1.filter(_._2._4 == 2).map(v => (v._1, new LabeledPoint(v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3))))
    val predictSet: RDD[(String, Double, Vector)] = imei_hasfeature_rdd1.filter(_._2._4 == 2).map(v => (v._1, v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3)))
    val predictSet_DF: DataFrame = hiveContext.createDataFrame(predictSet).toDF("imei", "label", "features")
    println("\n\n******************** The number of predictSet: " + predictSet.count + "  *********************\n\n")

    // val trainSet: RDD[(String, LabeledPoint)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, new LabeledPoint(v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3))))
    val trainSet: RDD[(String, Double, Vector)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3)))
    val trainSet_DF: DataFrame = hiveContext.createDataFrame(trainSet).toDF("imei", "label", "features")
    println("\n\n******************** The number of trainSet: " + trainSet.count + "  *********************\n\n")
    (trainSet_DF, predictSet_DF)
  }

  def build_model_based_df(
                   data_set: (DataFrame, DataFrame)
                 ): LogisticRegressionModel = {
    println("\n\n ********************* Build Model *************************")
    val trainSet: DataFrame = data_set._1
    val lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("label").setMaxIter(1000)

    val temp: Array[DataFrame] = trainSet.randomSplit(Array(0.8, 0.2), seed = 1234L)
    val train_set: DataFrame = temp(0).cache()
    val vaild_set: DataFrame = temp(1).cache()
    println("********************* Data set number: " + train_set.count() + " *************************")
    println("********************* label_0 count: " + train_set.filter(train_set("label") === 0).count() + " *******************")
    println("********************* label_1 count: " + train_set.filter(train_set("label") === 1).count() + " ******************* \n\n")

    val lr_model = lr.fit(train_set.select("label", "features"))

    val trainingSummary = lr_model.summary
    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Set the model threshold to maximize F-Measure
    val fMeasure = binarySummary.fMeasureByThreshold // [threshold, F-Measure]
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.filter(fMeasure("F-Measure") === maxFMeasure).select("threshold").head().getDouble(0)
    lr_model.setThreshold(bestThreshold)
    return lr_model
  }

//  def build_model_based_df_pipeline(
//                            data_set: (DataFrame, DataFrame)
//                          ): LogisticRegressionModel = {
//    println("\n\n ********************* Build Model *************************")
//    val trainSet: DataFrame = data_set._1
//    val lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("label")
//    // val pipeline = new Pipeline().setStages(Array(lr))
//    val param_grid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.01, 0.5, 2.0)).addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).addGrid(lr.maxIter, Array(50, 100)).build()
//    val cv = new CrossValidator().setEstimator(lr).setEvaluator(new BinaryClassificationEvaluator()).setEstimatorParamMaps(param_grid).setNumFolds(3)
//
//    val temp: Array[DataFrame] = trainSet.randomSplit(Array(0.8, 0.2), seed = 1234L)
//    val train_set: DataFrame = temp(0).cache()
//    val vaild_set: DataFrame = temp(1).cache()
//    println("********************* Data set number: " + train_set.count() + " *************************")
//    println("********************* label_0 count: " + train_set.filter(train_set("label") === 0).count() + " *******************")
//    println("********************* label_1 count: " + train_set.filter(train_set("label") === 1).count() + " ******************* \n\n")
//
//    val cvModel = cv.fit(train_set.select("label", "features"))
//
//    return lrModel
//  }

  def predict(model: LogisticRegressionModel, data_set: DataFrame): RDD[(Double, Double)] = {
    val prediction: RDD[(Double, Double)] = model.transform(data_set).select("label", "prediction").rdd.map(v => (v.getDouble(0), v.getDouble(1)))
    return prediction
  }

  def report_performance(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)
    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")
    return (au_roc, accuracy)
  }
}
