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

  case class Info_save(default_thred: Double,
                  AUC_default_thred: Double,
                  ACCU_default_thred: Double,
                  select_thred: Double,
                  AUC_select_thred: Double,
                  ACCU_select_thred: Double,
                  predict_M: Double,
                  predict_F: Double,
                  M_F_ratio: Double,
                  merged_predict_and_idCard_M: Double,
                  merged_predict_and_idCard_F: Double,
                  merged_M_F_ratio: Double,
                  ACCU_idcard_full: Double,
                  ACCU_idcard_part: Double)

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
    // val today = "20171127"
    val today = args(0) // mai
    val year: Int = today.substring(0, 4).trim.toInt
    val month: Int = today.substring(4, 6).trim.toInt
    val day: Int = today.substring(6, 8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month - 1, day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    println("\n************************ yestoday_Date: " + yestoday_Date + "***********************\n")
    val splitChar: String = "\u0001"

    // id_card data
    val id_card_info_full_table: String = "algo.yf_age_gender_accord_idcard_v2"
    val id_card_info_part_table: String = "algo.yf_sex_label_known_and_only_in_idcard"
    val id_card_data = get_idcard_from_table(hiveContext, id_card_info_full_table, id_card_info_part_table)

    // flyme sex knonwn data
    val sex_know_table_name: String = "algo.yf_imei_sex_for_build_sexmodel_flyme"
    //app install topK
    val topK: Int = 30000
    // app install feature by imei
    val imei_feature_table_name: String = "algo.yf_sex_model_imei_app_install_features_on_" + topK.toString + "dims"


    // prepare data and train model
    val dim_num: Int = topK
    val num_classes: Int = 2
    val select_threshold_flag: Boolean = true
    val train_pred_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = getTrainSet_predictSet(hiveContext, sex_know_table_name, imei_feature_table_name, dim_num, splitChar, yestoday_Date)
    val model: (LogisticRegressionModel, ArrayBuffer[Double]) = build_model(hiveContext, train_pred_set, num_classes, select_threshold_flag, yestoday_Date)

    // predict on data set of imei
    val predictions: (RDD[(String, String)], ArrayBuffer[Double]) = predict(hiveContext, model._1, train_pred_set, yestoday_Date)

    // evaluation on data set of id card
    val accu_on_id_card = evaluation_on_id_card(hiveContext, id_card_data._1, id_card_data._2, predictions._1)

    // merged idcard info with predictions
    val merged_predictions = merge_id_card_and_prediction(hiveContext, predictions._1, id_card_data._1)

    // write result to db
    write_result_to_DB(hiveContext, model._2, predictions._2, merged_predictions._2, accu_on_id_card, merged_predictions._1, yestoday_Date)
  }

  def write_result_to_DB(hiveContext: HiveContext,
                         train_info: ArrayBuffer[Double],
                         pred_info: ArrayBuffer[Double],
                         merged_info: ArrayBuffer[Double],
                         accu_on_id_card: ArrayBuffer[Double],
                         merged_result: RDD[(String, String)],
                         yestoday_Date: String
                        ) = {
    // store the result
    import hiveContext.implicits._
    println("\n\n ********************* merged result count: " + merged_result.count() + " ********************* ")
    val pre_df: DataFrame = merged_result.map(v => Imei_sex(v._1.toString, v._2.toString)).toDF
    println("********************* merged result DF count: " + pre_df.count() + " ********************* \n\n")
    pre_df.registerTempTable("prediction")

    println("\n\n ********************* (Strarting)Insert result to yf table ********************* ")
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
    println("********************* (Done)Insert result to yf table *********************\n\n ")

    // insert the predict result to latest table, and another job will write the result
    // to final sex table
    println("\n\n ********************* (Strarting)Insert result to final table ********************* ")
    val sex_last_pre_tableName: String = "algo.sex_model_latest_predictionAll"
    // val drop_sex_last_pre_table_sql: String = "drop table if exists " + sex_last_pre_tableName
    val create_sex_last_pre_tableSql: String =
      "create table if not exists " +
        sex_last_pre_tableName +
        " (imei string, sex string) stored as textfile"
    val insert_sex_last_pre_tableSql: String =
      "insert overwrite table " +
        sex_last_pre_tableName +
        " select * from prediction"
    // hiveContext.sql(drop_sex_last_pre_table_sql)
    hiveContext.sql(create_sex_last_pre_tableSql)
    hiveContext.sql(insert_sex_last_pre_tableSql)
    println("********************* (Done)Insert result to final table *********************\n\n ")


    // insert information to db
    println("\n\n ********************* (Strarting)Insert performance information to db ********************* ")
    val info = train_info ++ pred_info ++ merged_info ++ accu_on_id_card
    val performance_info_df = Seq(info).map(v => Info_save(v(0), v(1), v(2), v(3), v(4), v(5), v(6), v(7), v(8), v(9), v(10), v(11), v(12), v(13))).toDF()
    performance_info_df.registerTempTable("temp_table")
    val performance_table_name: String = "algo.yf_sex_model_performance_new_1"
    val creat_table_sql: String = "create table if not exists " + performance_table_name + "(default_thred double, AUC_default_thred double, ACCU_default_thred double, select_thred double, AUC_select_thred double, ACCU_select_thred double, pred_M double, pred_F double, ratio_M_F double, merged_pred_M double, merged_pred_F double, merged_ratio_M_F double, ACCU_idcard_full double, ACCU_idcard_part double) partitioned by (stat_date string) stored as textfile"
    val insert_table_sql: String = "insert overwrite table " + performance_table_name + " partition(stat_date = " + yestoday_Date + " ) select * from temp_table"
    hiveContext.sql(creat_table_sql)
    hiveContext.sql(insert_table_sql)
    println("********************* (Done)Insert performance information to db *********************\n\n ")
  }


  def predict(hiveContext: HiveContext,
              model: LogisticRegressionModel,
              train_pre: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]),
              yestoday_Date: String): (RDD[(String, String)], ArrayBuffer[Double]) = {

    val prediction: RDD[(String, String)] = train_pre._2.map(v => {
      if (model.predict(v._2.features) == 1d)
        (v._1, "male")
      else
        (v._1, "female")
    })
    val pre: RDD[(String, String)] = train_pre._1.map(v => (v._1, if (v._2.label == 1d) "male" else "female")).union(prediction).cache()

    val pred_male_count = pre.filter(_._2 == "male").count()
    val pred_female_count = pre.filter(_._2 == "female").count()
    println("\n\n***************** The mumber of prediction: " + pre.count() + " *************")
    println("***************** The mumber of male in prediction: " + pred_male_count + " *************")
    println("***************** The mumber of female in prediction: " + pred_female_count + " *************")
    println("***************** The ratio of male vs female: " + pred_male_count * 1.0 / pred_female_count + " ************* \n\n")

    // insert the model trainning detail to table
    import hiveContext.implicits._
    val info_predict: ArrayBuffer[Double] = ArrayBuffer[Double](
      pred_male_count.toDouble,
      pred_female_count.toDouble,
      pred_male_count * 1.0 / pred_female_count)

    return (pre, info_predict)
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
    val imei_sexLabel: RDD[(String, Double)] = hiveContext.sql(select_know_sex_sql).map(_.mkString(splitChar)).filter(_.split(splitChar).length == 2).map(v => {
      val array: Array[String] = v.trim.split(splitChar)
      (array(0).trim, array(1).trim.toDouble)
    })
    println("\n\n********************* The number of imei known sex: " + imei_sexLabel.count() + " ***********************")

    val select_imei_feature_sql: String = "select * from " + imei_feature_table_name + " where stat_date=" + yestoday_Date
    val imei_feature_rdd: RDD[(String, String)] = hiveContext.sql(select_imei_feature_sql).rdd.map(v => (v(0).toString, v(1).toString))
    println("********************* The date of imei: " + yestoday_Date + " **********************")
    println("********************* The number of imei: " + imei_feature_rdd.count() + " ***********************")

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
    println("******************** The number of predictSet: " + predictSet.count + "  *********************")

    val trainSet: RDD[(String, LabeledPoint)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, new LabeledPoint(v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3))))
    // val trainSet: RDD[(String, Double, Vector)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3)))
    // val trainSet_DF: DataFrame = hiveContext.createDataFrame(trainSet).toDF("imei", "label", "features")
    println("******************** The number of trainSet: " + trainSet.count + "  *********************\n\n")
    (trainSet, predictSet)
  }


  def build_model(
                   hiveContext: HiveContext,
                   data_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]),
                   classes_num: Int,
                   select_threshold_flag: Boolean,
                   yestoday_Date: String
                 ): (LogisticRegressionModel, ArrayBuffer[Double]) = {
    println("\n\n ********************* Build Model *************************")
    val trainSet: RDD[(String, LabeledPoint)] = data_set._1 // balance dataset

    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = trainSet.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()
    println("********************* Data set number: " + train_rdd.count() + " *************************")
    println("********************* label_0 count: " + train_rdd.filter(_._2.label == 0).count() + " *******************")
    println("********************* label_1 count: " + train_rdd.filter(_._2.label == 1).count() + " *******************\n\n")

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))

    println("\n\n****************** The Default threshold performance ***************************")
    val defalut_threshold: Double = model.getThreshold.get
    println("************* The Default threshold: " + defalut_threshold + "*****************")
    val valid_result_default_threshold: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val ROC_default_thred = ROC(valid_result_default_threshold.map(_._2))
    val AUC_default_thred = ROC_default_thred._1
    val ACCU_default_thred = ROC_default_thred._2
    println("********************** AUROC_default_threshold: " + AUC_default_thred + " *********************")
    println("********************** Accuracy_default_threshold: " + ACCU_default_thred + " *********************\n\n")

    println("\n\n****************** The Best threshold performance ***************************")
    // choosing the best threshold
    model.clearThreshold()
    val valid_result_score: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val binaryClassificationMetrics = new BinaryClassificationMetrics(valid_result_score.map(_._2))
    val fMeasure = binaryClassificationMetrics.fMeasureByThreshold()
    val best_threshold = fMeasure.reduce((a, b) => {
      if (a._2 < b._2) b else a
    })._1

//    val threshold_set = 0.52
//    if (best_threshold < threshold_set) {
//      best_threshold = threshold_set
//    }

    model.setThreshold(best_threshold)
    println("************* The Best threshold: " + model.getThreshold.get + "*****************")
    val valid_result_select_threshold: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val ROC_select_thred = ROC(valid_result_select_threshold.map(_._2))
    val AUC_select_thred = ROC_select_thred._1
    val ACCU_select_thred = ROC_select_thred._2
    println("********************** AUROC_select_threshold: " + AUC_select_thred + " *********************")
    println("********************** Accuracy_select_threshold: " + ACCU_select_thred + " *********************\n\n")

    if (!select_threshold_flag) {
      model.setThreshold(defalut_threshold)
    }
    println("\n\n************** Finally Threshold: " + model.getThreshold.get + " ********************** \n\n")

    // insert the model trainning detail to table
    val info_train: ArrayBuffer[Double] = ArrayBuffer[Double](
      defalut_threshold,
      AUC_default_thred,
      ACCU_default_thred,
      best_threshold,
      AUC_select_thred,
      ACCU_select_thred)

    return (model, info_train)
  }

  def ROC(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)

    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")
    return (au_roc, accuracy)
  }


  def get_idcard_from_table(hiveContext: HiveContext,
                             id_card_info_full_table: String,
                             id_card_info_part_table: String): (DataFrame, DataFrame) = {
    val select_latest_date_sql = "show PARTITIONS " + id_card_info_full_table
    val latest_date: String = hiveContext.sql(select_latest_date_sql).map(v => v(0).toString.split("=")(1).toInt).collect().sortWith((a, b) => a > b)(0).toString
    println("\n\n***************** The latest date of id_card_info_full_table: " + latest_date.toString() + " *************")

    val select_id_info_full_sql: String = "SELECT imei, gender as sex_idcard from " + id_card_info_full_table + " where stat_date=" + latest_date
    val select_id_info_part_sql: String = "select imei, sex as sex_idcard from " + id_card_info_part_table

    val id_card_info_full_df: DataFrame = hiveContext.sql(select_id_info_full_sql)
    val id_card_info_part_df: DataFrame = hiveContext.sql(select_id_info_part_sql)
    println("***************** id_card_info_full_table count: " + id_card_info_full_df.count() + " *************")
    println("***************** id_card_info_part_df count: " + id_card_info_part_df.count() + " *************\n\n")

    return (id_card_info_full_df, id_card_info_part_df)
  }

  def merge_id_card_and_prediction(hiveContext: HiveContext,
                                   pred: RDD[(String, String)],
                                   id_card_full_df: DataFrame): (RDD[(String, String)], ArrayBuffer[Double]) ={
    val id_card_info_full_rdd = id_card_full_df.rdd.map(v => (v.getString(0), v.getString(1))).reduceByKey((a, b) => a)

    val temp = id_card_info_full_rdd.subtractByKey(pred)

    val merged_pred = pred.leftOuterJoin(id_card_info_full_rdd).map(v => {
      if(v._2._2.isDefined && (v._2._1 != v._2._2.get))
        (v._1, v._2._2.get)
      else
        (v._1, v._2._1)
    }).cache().union(temp)

    val merged_pred_male_count = merged_pred.filter(_._2 == "male").count()
    val merged_pred_female_count = merged_pred.filter(_._2 == "female").count()
    val M_F_ration = merged_pred_male_count * 1.0 / merged_pred_female_count
    println("\n\n***************** The mumber of merged prediction: " + merged_pred.count() + " ************* ")
    println("***************** The mumber of male in merged prediction: " + merged_pred_male_count + " *************")
    println("***************** The mumber of female in merged prediction: " + merged_pred_female_count + " *************")
    println("***************** The ratio of male vs female: " + M_F_ration + " ************* \n\n")

    // insert the model trainning detail to table
    val info_merged: ArrayBuffer[Double] = ArrayBuffer[Double](
      merged_pred_male_count.toDouble,
      merged_pred_female_count.toDouble,
      M_F_ration.toDouble)
    return (merged_pred, info_merged)
  }

  def evaluation_on_id_card(hiveContext: HiveContext,
                            id_card_info_full_df: DataFrame,
                            id_card_info_part_df: DataFrame,
                            predictions: RDD[(String, String)]): ArrayBuffer[Double] ={
    import hiveContext.implicits._
    val predictions_df: DataFrame = predictions.map(v => Imei_sex(v._1, v._2)).toDF
    // evaluation using full data of id card
    val imei_matched_0 = id_card_info_full_df.join(predictions_df, id_card_info_full_df("imei") === predictions_df("imei"))
    val correct_pred_0 = imei_matched_0.rdd.filter(v => v(1).toString == v(3).toString)
    val accuracy_0 = correct_pred_0.count() * 1.0 / imei_matched_0.count()
    println("\n\n***************** Evaluation using full data of id card (accuracy):" + accuracy_0  + "************")

    // evaluation using part data of id card
    val imei_matched_1 = id_card_info_part_df.join(predictions_df, id_card_info_part_df("imei") === predictions_df("imei"))
    val correct_pred_1 = imei_matched_1.rdd.filter(v => v(1).toString == v(3).toString)
    val accuracy_1 = correct_pred_1.count() * 1.0 / imei_matched_1.count()
    println("***************** Evaluation using part data of id card (accuracy):" + accuracy_1  + "************ \n\n")
    val res = ArrayBuffer(accuracy_0, accuracy_1)
    return res
  }
}
