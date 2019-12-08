import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel, classification}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, Tokenizer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import util.Stopwatch
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.classification.NaiveBayes



class Model extends Stopwatch {



  def train_lr(inputPath: String, outputFolder: String): PipelineModel = {

    // Setup SparkSession
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SentimentAnalysis")

    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    ss.sparkContext.setLogLevel("error")
    time {

      // Read raw dataset
      val rawDf = ss
          .read
          .option("header", true)
          .option("delimiter", ",")
          .csv(inputPath)

      rawDf.persist()
      rawDf.show(10)
      println(s"Number of rows: ${rawDf.count()}")



      // Basic data cleaning

      val df = rawDf.withColumn("label", toDouble(rawDf("label")))
          .withColumn("text", rawDf("text"))
          .drop("SentimentSource", "\uFEFFItemID", "SentimentText", "Sentiment")
          df.show(10)

      df.withColumn("text_length", length(df("text")))
          .groupBy("label")
          .avg("text_length")
          .toDF("label", "avg_text_length")
          .show()



      // Split data on the train and test datasets

      val Array(train, test) = df.randomSplit(Array(0.8, 0.2))

      // Use tokenizer for splitting text on the words
      // Tokenization is the task of chopping it up into pieces, called tokens ,
      // perhaps at the same time throwing away certain characters, such as punctuation.

      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")

      // Use CountVectorizer for converting words into vectors of integers

      val vectorizer = new CountVectorizer() //
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")

      // Logistic regression

      val lr = new LogisticRegression()
        .setRegParam(0.001)

      val pipeline_lr = new Pipeline()
        .setStages(Array(tokenizer, vectorizer, lr))

      // Train the model
      val model_lr = pipeline_lr.fit(train)
      model_lr.write.overwrite().save(outputFolder)


      // Evaluate the model
      var totalCorrect_lr = 0.0
      val result_lr = model_lr
        .transform(test)
        .select("prediction", "label")
        .collect()


      result_lr.foreach { case Row(prediction, label) => if (prediction == label) totalCorrect_lr += 1 }
      val accuracy_lr = totalCorrect_lr / result_lr.length
      println(s"Accuracy Logistic Regression: $accuracy_lr")

      model_lr

    }
  }

  def train_dt(inputPath: String, outputFolder: String): PipelineModel = {

    // Setup SparkSession

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SentimentAnalysis")

    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    ss.sparkContext.setLogLevel("error")
    time {

      // Read raw dataset

      val rawDf = ss
        .read
        .option("header", true)
        .option("delimiter", ",")
        .csv(inputPath)

      rawDf.persist()
      rawDf.show(10)
      println(s"Number of rows: ${rawDf.count()}")

      // Basic data cleaning

      val df = rawDf.withColumn("label", toDouble(rawDf("label")))
        .withColumn("text", rawDf("text"))
        .drop("SentimentSource", "\uFEFFItemID", "SentimentText", "Sentiment")

      df.show(10)

      df.withColumn("text_length", length(df("text")))
        .groupBy("label")
        .avg("text_length")
        .toDF("label", "avg_text_length")
        .show()

      // Split data on the train and test datasets

      val Array(train, test) = df.randomSplit(Array(0.8, 0.2))

      // Use tokenizer for splitting text on the words

      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")

      // Use CountVectorizer for converting words into vectors of integers

      val vectorizer = new CountVectorizer() //
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")

      // Decision Tree Classifier is one of the most simple algorithms for classification problems


      val dt = new DecisionTreeClassifier()

      val pipeline_dt = new Pipeline()
        .setStages(Array(tokenizer, vectorizer, dt))

      // Train the model
      val model_dt = pipeline_dt.fit(train)
      model_dt.write.overwrite().save(outputFolder)


      // Evaluate the model
      var totalCorrect_dt = 0.0
      val result_dt = model_dt
        .transform(test)
        .select("prediction", "label")
        .collect()

      result_dt.foreach { case Row(prediction, label) => if (prediction == label) totalCorrect_dt += 1 }
      val accuracy_dt = totalCorrect_dt / result_dt.length
      println(s"Accuracy Decision Tree Classifier: $accuracy_dt")

      model_dt

    }

  }

  def train_svm(inputPath: String, outputFolder: String): PipelineModel = {

    // Setup SparkSession

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SentimentAnalysis")

    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    ss.sparkContext.setLogLevel("error")
    time {

      // Read raw dataset

      val rawDf = ss
        .read
        .option("header", true)
        .option("delimiter", ",")
        .csv(inputPath)

      rawDf.persist()
      rawDf.show(10)
      println(s"Number of rows: ${rawDf.count()}")

      // Basic data cleaning

      val df = rawDf.withColumn("label", toDouble(rawDf("label")))
        .withColumn("text", rawDf("text"))
        .drop("SentimentSource", "\uFEFFItemID", "SentimentText", "Sentiment")

      df.show(10)

      df.withColumn("text_length", length(df("text")))
        .groupBy("label")
        .avg("text_length")
        .toDF("label", "avg_text_length")
        .show()

      // Split data on the train and test datasets

      val Array(train, test) = df.randomSplit(Array(0.8, 0.2))

      // Use tokenizer for splitting text on the words

      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")

      // Use CountVectorizer for converting words into vectors of integers

      val vectorizer = new CountVectorizer() //
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")

             // SVM Classifier

            val svm = new LinearSVC()

            val pipeline_svm = new Pipeline()
              .setStages(Array(tokenizer, vectorizer, svm))

            // Train the model
            val model_svm = pipeline_svm.fit(train)
            model_svm.write.overwrite().save(outputFolder)


            // Evaluate the model
            var totalCorrect_svm = 0.0
            val result_svm = model_svm
              .transform(test)
              .select("prediction", "label")
              .collect()

            result_svm.foreach { case Row(prediction, label) => if (prediction == label) totalCorrect_svm += 1 }
            val accuracy_svm = totalCorrect_svm / result_svm.length
            println(s"Accuracy SVM Classifier: $accuracy_svm")

      model_svm

    }

  }


  def train_nb(inputPath: String, outputFolder: String): PipelineModel = {

    // Setup SparkSession

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SentimentAnalysis")

    val ss = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    ss.sparkContext.setLogLevel("error")
    time {

      // Read raw dataset

      val rawDf = ss
        .read
        .option("header", true)
        .option("delimiter", ",")
        .csv(inputPath)

      rawDf.persist()
      rawDf.show(10)
      println(s"Number of rows: ${rawDf.count()}")

      // Basic data cleaning

      val df = rawDf.withColumn("label", toDouble(rawDf("label")))
        .withColumn("text", rawDf("text"))
        .drop("SentimentSource", "\uFEFFItemID", "SentimentText", "Sentiment")

      df.show(10)

      df.withColumn("text_length", length(df("text")))
        .groupBy("label")
        .avg("text_length")
        .toDF("label", "avg_text_length")
        .show()

      // Split data on the train and test datasets

      val Array(train, test) = df.randomSplit(Array(0.8, 0.2))

      // Use tokenizer for splitting text on the words

      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")

      // Use CountVectorizer for converting words into vectors of integers

      val vectorizer = new CountVectorizer() //
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")

      // Naive Bayes Classifier

            val nb = new NaiveBayes()

            val pipeline_nb = new Pipeline()
              .setStages(Array(tokenizer, vectorizer, nb))

            // Train the model
            val model_nb = pipeline_nb.fit(train)
            model_nb.write.overwrite().save(outputFolder)


            // Evaluate the model
            var totalCorrect_nb = 0.0
            val result_nb = model_nb
              .transform(test)
              .select("prediction", "label")
              .collect()

            result_nb.foreach { case Row(prediction, label) => if (prediction == label) totalCorrect_nb += 1 }
            val accuracy_nb = totalCorrect_nb / result_nb.length
            println(s"Accuracy Naive Bayes: $accuracy_nb")

           model_nb
    }

  }

  private val toDouble = udf[Double, String](_.toDouble)
  private val length = udf[Long, String](_.length)

}

object Model extends App {
  val model_lr = new Model().train_lr("/Users/abhipatodi/Desktop/NorthEastern\\ Subjects/Big\\ Data\\ Using\\ Scala/Final_Project/Scala_Sentiment_Analysis/train.csv", "/Users/abhipatodi/Downloads/Scala_models/lr_model")
  val model_dt = new Model().train_dt("/Users/abhipatodi/Desktop/NorthEastern\\ Subjects/Big\\ Data\\ Using\\ Scala/Final_Project/Scala_Sentiment_Analysis/train.csv", "/Users/abhipatodi/Downloads/Scala_models/dt_model")
  val model_nb = new Model().train_nb("/Users/abhipatodi/Desktop/NorthEastern\\ Subjects/Big\\ Data\\ Using\\ Scala/Final_Project/Scala_Sentiment_Analysis/train.csv", "/Users/abhipatodi/Downloads/Scala_models/nb_model")
  val model_svm = new Model().train_svm("/Users/abhipatodi/Desktop/NorthEastern\\ Subjects/Big\\ Data\\ Using\\ Scala/Final_Project/Scala_Sentiment_Analysis/train.csv", "/Users/abhipatodi/Downloads/Scala_models/svm_model")



}
