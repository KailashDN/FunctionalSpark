import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import util.Stopwatch



object Test extends App with Stopwatch {
 // private val savedModelFolder = "/Users/abhipatodi/Downloads/model"

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("SentimentAnalysisTest")

  val ss = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
  ss.sparkContext.setLogLevel("error")

  val df = ss.read
    .option("header", true)
    .csv("./test.csv")
    df.show()

  println("------------------- Prediction for Logistic Regression -------------------")
  val pipelineModel_lr = PipelineModel.read.load("./models/lr_model")
  time {
    val lrpredictions = pipelineModel_lr.transform(df)
    lrpredictions.select("text", "prediction","words").show(50)

  }

  println("------------------- Prediction for Decision Tree -------------------")
  val pipelineModel_dt = PipelineModel.read.load("./models/dt_model")
  time {
    val drpredictions = pipelineModel_dt.transform(df)
    drpredictions.select("text", "prediction","words").show(50)

   // pipelineModel_dt.transform(df).foreach(r => println(s"Text: ${r(1)}, Score: ${r(r.length - 1)}"))
  }

  println("------------------- Prediction for SVM Classifier -------------------")
  val pipelineModel_svm = PipelineModel.read.load("./models/svm_model")
  time {
    val svmpredictions = pipelineModel_svm.transform(df)
    svmpredictions.select("text", "prediction","words").show(50)
    //pipelineModel_svm.transform(df).foreach(r => println(s"Text: ${r(1)}, Score: ${r(r.length - 1)}"))
  }

  println("------------------- Prediction for Naive Bayes Classifier -------------------")

  val pipelineModel_nb = PipelineModel.read.load("./models/nb_model")
  time {
    val nbpredictions = pipelineModel_nb.transform(df)
    nbpredictions.select("text", "prediction","words").show(50)

   // pipelineModel_nb.transform(df).foreach(r => println(s"Text: ${r(1)}, Score: ${r(r.length - 1)}"))
  }

}
