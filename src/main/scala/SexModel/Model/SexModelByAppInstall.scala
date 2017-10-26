package SexModel.Model

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
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
    //val today = "20171020"
    val today = args(0) // mai
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year,month-1,day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)


    val splitChar: String = "\u0001"
    val model_save_dir: String = "hdfs://mzcluster/user/mzsip/yuanfang/algo/sex_model/models/app_install/" + yestoday_Date + "/"

    // sex knonwn data
    val sex_know_table_name: String = "algo.yf_imei_sex_for_build_sexmodel_flyme"
    //app install topK
    val topK: Int = 30000
    // app install feature by imei
    val imei_feature_table_name: String = "algo.yf_sex_model_imei_app_install_features_on_" + topK.toString + "dims"
    // val create_predict_table_name: String = "algo.yf_sex_model_app_install_predict_on_" + topK.toString + "dims"
    val create_predict_table_name: String = "algo.yf_sex_model_app_install_predict_on_" + topK.toString + "_new_2"

    // prepare data and train model
    val dim_num: Int = topK
    val train_pred_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = getTrainSet_predictSet(hiveContext, sex_know_table_name, imei_feature_table_name, dim_num, splitChar, yestoday_Date)
    val model: (LogisticRegressionModel, RDD[(String, (Double, Double))]) = build_model_1(train_pred_set, 2)

    report_model_performance(model._2)
    // predict on data set of imei
    val pred: RDD[(String, String)] = predict(model._1, train_pred_set)

    import hiveContext.implicits._
    val pre_df: DataFrame = pred.repartition(100).map(v => Imei_sex(v._1, v._2)).toDF

    println("\n\n ********************* (Strarting)Insert result to yf table *********************\n\n ")
    pre_df.registerTempTable("prediction")
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

  /**
   * 将用户的的行为数据整理成两部分数据，一部分是性别标签已知的样本，一部分是性别标签位置的样本
   * @param hiveContext                用于读取hive库中的表
   * @param know_sex_table_name        性别标签已知的imei，格式是: (imei, sex)  sex=1表示男   sex=0表示女
   * @param imei_feature_table_name    用户的行为数据，格式是：（imei, feature)
   * @param dim_num                    用户行为对应的维度
   * @param splitChar                  字段临时分隔符
   * @return                           返回两部分数据，一个是标签位置的样本，一个是标签已知的样本，
   *                                   用标签已知的样本建立模型，然后预测标签位置的样本
   */
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
    println("\n\n******************** The number of predictSet: " + predictSet.count + "  *********************\n\n")

    val trainSet: RDD[(String, LabeledPoint)] = imei_hasfeature_rdd1.filter(_._2._4 == 3).map(v => (v._1, new LabeledPoint(v._2._1, Vectors.sparse(dim_num, v._2._2, v._2._3))))
    println("\n\n******************** The number of trainSet: " + trainSet.count + "  *********************\n\n")
    (trainSet, predictSet)
  }

  def deleteIfexists(model_save_dir: String): Unit = {
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(URI.create(model_save_dir), conf, "mzsip")
    val path: Path = new Path(model_save_dir)
    if (fs.exists(path))
      fs.delete(path, true)
  }

  /**
   * 将标签已知的数据分为训练集和测试集，训练集用于建立模型，测试集用于检验模型效果
   * @param sc                       当前的spark运行上下文
   * @param trainSet                 标签已知的样本
   * @param model_save_dir           模型的保存路径
   * @return                         返回一个logistic二分类模型和这个模型的对应的分类阈值
   */
  def buildModel(sc: SparkContext, trainSet: RDD[(String, LabeledPoint)], model_save_dir: String): (Double, LogisticRegressionModel) = {
    deleteIfexists(model_save_dir)
    var ret_model: LogisticRegressionModel = null
    val rdds: Array[RDD[(String, LabeledPoint)]] = trainSet.randomSplit(Array(0.8, 0.2))
    var trainRDD: RDD[(String, LabeledPoint)] = rdds(0).cache()
    val predictRDD: RDD[(String, LabeledPoint)] = rdds(1).cache()
    val num_1: Long = predictRDD.filter(_._2.label == 1d).count()
    val num_0: Long = predictRDD.filter(_._2.label == 0d).count()

    val thresholds: Array[Double] = (for (i <- 1 until 100) yield i.toDouble / 100).toArray
    val split: String = "\t"
    var model: LogisticRegressionModel = null
    var prediction: RDD[(String, (Double, Double))] = sc.emptyRDD[(String, (Double, Double))]
    var maxVar: Double = 0
    var goodthreshold: Double = 0
    var threshold: Double = 0
    val iterationNum: Int = 10
    var k: Int = 0
    while (k < iterationNum) {
      var predictInfo: ArrayBuffer[String] = new ArrayBuffer[String]()
      val train1: Long = trainRDD.filter(_._2.label == 1d).count()
      val train0: Long = trainRDD.filter(_._2.label == 0d).count()
      val train_info: String = "train1: " + train1 + split + "train0: " + train0
      predictInfo += train_info
      var tmpVariance: Double = 0
      model = new LogisticRegressionWithLBFGS().run(trainRDD.map(_._2))
      model.clearThreshold()
      model.save(sc, model_save_dir + "model_" + k.toString + "/model")
      prediction = predictRDD.map(v => (v._1, (v._2.label, model.predict(v._2.features)))).cache()
      println(s"\n\n&&&&&&&&&&&&&&&&&  第 $k 次模型效果！！ &&&&&&&&&&&&&&&&&")
      println(train_info)
      for (i <- 0 until thresholds.length) {
        val num_11: Long = prediction.filter(v => v._2._1 == 1d && v._2._2 > thresholds(i)).count()
        val num_10: Long = prediction.filter(v => v._2._1 == 1d && v._2._2 <= thresholds(i)).count()
        val num_00: Long = prediction.filter(v => v._2._1 == 0d && v._2._2 <= thresholds(i)).count()
        val num_01: Long = prediction.filter(v => v._2._1 == 0d && v._2._2 > thresholds(i)).count()
        val pre1: Double = if (num_11 + num_01 == 0d) 0d else num_11.toDouble / (num_11 + num_01)
        val pre0: Double = if (num_00 + num_10 == 0d) 0d else num_00.toDouble / (num_00 + num_10)
        val cov1: Double = if (num_1 == 0) 0d else num_11.toDouble / num_1
        val cov0: Double = if (num_0 == 0) 0d else num_00.toDouble / num_0
        val variance: Double = math.sqrt(cov1 * cov0)
        if (variance > tmpVariance) {
          tmpVariance = variance
          goodthreshold = thresholds(i)
        }
        val info: String =
          "threshold: " + thresholds(i) + split +
          "num_11:" + num_11 + split +
          "num_10: " + num_10 + split +
          "num_00: " + num_00 + split +
          "num_01: " + num_01 + split +
          "pre1: " + pre1 + split +
          "pre_0: " + pre0 + split +
          "cov1: " + cov1 + split +
          "cov0: " + cov0
        predictInfo += info
        // println(info)
      }
      val pre_info: String =
        "num_1: " + num_1 + split +
        "num_0: " + num_0 + split +
        "tmpVariance: " + tmpVariance + split +
        "goodthreshold: " + goodthreshold
      predictInfo += pre_info
      // println(pre_info)

      sc.parallelize(predictInfo.toSeq)
        .repartition(1)
        .saveAsTextFile(model_save_dir + "model_" + k.toString + "/model_analysis")

      if (tmpVariance > maxVar) {
        maxVar = tmpVariance
        threshold = goodthreshold
        ret_model = model
      }

      val trainPrediction: RDD[(String, (Double, Double))] = trainRDD.map(v => {
        (v._1, (v._2.label, model.predict(v._2.features)))
      })
      val correctUid: RDD[(String, Double)] = trainPrediction.filter(v => {
        (v._2._1 == 1.0 && v._2._2 >= goodthreshold) || v._2._1 == 0.0
      }).map(v => (v._1, v._2._1))
      val newTrainRDD: RDD[(String, LabeledPoint)] = correctUid.join(trainRDD).map(v => (v._1, v._2._2)).cache()
      trainRDD.unpersist()
      trainRDD.count()
      trainRDD = newTrainRDD
      prediction.unpersist()
      prediction.count()
      k += 1
    }
    println("\n\n@@@@@@@@@@@@@  maxVar: " + maxVar + split + "goodthreshold: " + goodthreshold + "  @@@@@@@@@@@@@@\n\n")
    (threshold, ret_model)
  }

  def build_model_1(
                   data_set: (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]),
                   classes_num: Int
                 ): (LogisticRegressionModel, RDD[(String, (Double, Double))]) = {
    println("\n\n ********************* Build Model *************************")
    val trainSet: RDD[(String, LabeledPoint)] = data_set._1 // balance dataset

    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = trainSet.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    println("********************* Data set number: " + train_rdd.count() + " *************************")
    println("********************* label_0 count: " + train_rdd.filter(_._2.label == 0).count() + " *******************")
    println("********************* label_1 count: " + train_rdd.filter(_._2.label == 1).count() + " ******************* \n\n")

    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()

    val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))
    val valid_result: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))

    // val multiclassMetrics = new MulticlassMetrics(valid_result.map(_._2))
    // println("\n\n ********************** Precision = " + multiclassMetrics.precision + " ********************* \n\n")
    // println("\n\n ********************** Recall = " + multiclassMetrics.recall + " ********************* \n\n")
    // for (i <- 0 to classes_num-1) {
    //   println("\n\n ********************** Precision_" + i + " = " + multiclassMetrics.precision(i) + " ********************* \n\n")
    // }

    // val binary_class_metrics = new BinaryClassificationMetrics(valid_result.map(_._2))
    // val roc = binary_class_metrics.roc()
    // val au_roc = binary_class_metrics.areaUnderROC()
    // println("\n\n ********************** AUROC: " + au_roc + " ********************* \n\n")

    return (model, valid_result)

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
