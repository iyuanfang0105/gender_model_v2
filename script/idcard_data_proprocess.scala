import java.util.Calendar
import java.text.SimpleDateFormat

//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD

//import org.apache.log4j.{Level, Logger}


case class UserAgeSex(user_id: Int, age_range: String, sex: String)

//// Initial spark
//val sparkConf = new SparkConf()
//val sc:SparkContext = new SparkContext(sparkConf)
//System.setProperty("user.name", "yuanfang")
//System.setProperty("HADOOP_USER_NAME", "yuanfang")
//sparkConf.setAppName("ALGO_yuanfang_idNumberAnalysis")
//sc.hadoopConfiguration.set("mapred.output.compress", "false")
//println("================== Initial Spark Done =========================")

// Initial Hive
val hiveContext = new HiveContext(sc)
hiveContext.setConf("mapred.output.compress", "false")
hiveContext.setConf("hive.exec.compress.output", "false")
hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
println("================== Initial HIVE Done =========================")
import hiveContext.implicits._

//// Initial Logger
//Logger.getLogger("org").setLevel(Level.ERROR)
//Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
//Logger.getLogger("akka").setLevel(Level.ERROR)
//Logger.getRootLogger().setLevel(Level.ERROR)
//println("================== Initial Logger Done =========================")

// get timestamp
val calendar: Calendar = Calendar.getInstance()
val currentYear: Int = calendar.get(Calendar.YEAR)
val yestoday: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

// function to get age_label_rdd
def getAgeLabelRDD(hiveContext: HiveContext, userCerificationInfoTable: String, currentYear: Int)
:RDD[(String, Double, Double, Double, String)] = {
  val selectDataSQL = "SELECT user_id, first_id_number, second_id_number from " + userCerificationInfoTable  + " where first_id_number REGEXP '^[0-9].*$'"
  // (user_id, first_id_number, second_id_number)
  // eg: (616	37110219850124	3)
  val resDF: DataFrame = hiveContext.sql(selectDataSQL)
  val resDFUniqByIdCard: DataFrame = resDF.dropDuplicates(Seq("first_id_number", "second_id_number"))

  // parse the result data frame
  val rddUserLabel: RDD[(String, Double, Double, Double, String)] = resDFUniqByIdCard.map(
    r => {
      val user_id: String = r.get(0).toString.trim

      var ageLabel: Int = 0
      var genderLabel: String = null

      // get the age label of user
      var birthYear: Int = 0
      if (r.get(1).toString.trim.length == 14)
        birthYear: Int = r.get(1).toString.trim.substring(6, 10).toInt
      if (r.get(1).toString.trim.length == 12)
        birthYear: Int = r.get(1).toString.trim.substring(6, 8).toInt + 1900

      val user_age = currentYear - birthYear

      if (user_age >= 7 && user_age <= 14)
        ageLabel = 1
      else if (user_age >= 15 && user_age <= 22)
        ageLabel = 2
      else if (user_age >= 23 && user_age <= 35)
        ageLabel = 3
      else if (user_age >= 36 && user_age <= 45)
        ageLabel = 4
      else if (user_age >= 46 && user_age <= 76)
        ageLabel = 5

      // get the gender label of user
      val genderInf: Int = r.get(2).toString.trim.toInt
      if (genderInf % 2 == 0)
        genderLabel = "female"
      else
        genderLabel = "male"

      (user_id, user_age, ageLabel, genderInf, genderLabel)
    }
  )
  rddUserLabel
}

val userCerificationInfoTable: String = "user_center.ods_uc_certification_info_c"
val userAgeLabel: RDD[(String, Double, Double, Double, String)] = getAgeLabelRDD(hiveContext, userCerificationInfoTable, currentYear)
val userAgeLabelFine: RDD[(String, Double, Double, Double, String)] = userAgeLabel.filter(v => v._2 >= 7).filter(v => v._2 <= 76)

// save the userAgeLabel to hive
val userAgeLabelDF: DataFrame = userAgeLabelFine.map(v => UserAgeSex(v._1.toInt, v._3.toString, v._5)).toDF()
val userAgeLabelTable: String = "algo.yf_age_gender_accord_IDCard"
val userAgeLabelTableTemp: String = "age_gender_result"
userAgeLabelDF.registerTempTable(userAgeLabelTableTemp)
//val creatUserAgeLabelSQL: String = "create table if not exists " + userAgeLabelTable + " (user_id bigint, age_range string, sex string) partitioned by (stat_date string) stored as textfile"
//val InsertUserAgeLabelSQL:String = "insert overwrite table " + userAgeLabelTable + " partition(stat_date = " + yestoday + ") select * from " + userAgeLabelTableTemp
val creatUserAgeLabelSQL: String = "create table if not exists " + userAgeLabelTable + " (user_id bigint, age_range string, sex string) stored as textfile"
val InsertUserAgeLabelSQL:String = "insert overwrite table " + userAgeLabelTable + " select * from " + userAgeLabelTableTemp

hiveContext.sql(creatUserAgeLabelSQL)
hiveContext.sql(InsertUserAgeLabelSQL)


// count the male and female
val maleNum = userAgeLabelFine.filter(v => v._5 == "male").count()
val femaleNum = userAgeLabelFine.filter(v => v._5 == "female").count()
println("================== Counting Gender Number =========================")
println("============== Male: " + maleNum + " Female: " + femaleNum + " Gender Total: " + (maleNum + femaleNum))
println("============== Male : Female  " + maleNum * 1.0 / femaleNum)

// count the age
val age_range_1 = userAgeLabelFine.filter(v => v._3 == 1).count()
val age_range_2 = userAgeLabelFine.filter(v => v._3 == 2).count()
val age_range_3 = userAgeLabelFine.filter(v => v._3 == 3).count()
val age_range_4 = userAgeLabelFine.filter(v => v._3 == 4).count()
val age_range_5 = userAgeLabelFine.filter(v => v._3 == 5).count()
val ageTotal = age_range_1 + age_range_2 + age_range_3 + age_range_4 + age_range_5
println("================== Counting Age Number =========================")
println("============== age_total: " + ageTotal)
println("============== age_range_1: " + age_range_1 + " ratio: " + age_range_1 * 1.0 / ageTotal )
println("============== age_range_2: " + age_range_2 + " ratio: " + age_range_2 * 1.0 / ageTotal )
println("============== age_range_3: " + age_range_3 + " ratio: " + age_range_3 * 1.0 / ageTotal )
println("============== age_range_4: " + age_range_4 + " ratio: " + age_range_4 * 1.0 / ageTotal )
println("============== age_range_5: " + age_range_5 + " ratio: " + age_range_5 * 1.0 / ageTotal )

val age_count = userAgeLabelFine.map(_._2).countByValue()
age_count.foreach(x => print(x))


