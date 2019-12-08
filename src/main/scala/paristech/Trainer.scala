package paristech

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{CountVectorizer, IDF, OneHotEncoderEstimator, RegexTokenizer, StopWordsRemover, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Trainer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP Spark : Trainer")
      .getOrCreate();

    // import in order to be able to use the $ notation
    // NB : this import must be done *after* the creation of the spark session !
    import spark.implicits._
    /*******************************************************************************
      *
      *       TP 3
      *
      *       - lire le fichier sauvegarde précédemment
      *       - construire les Stages du pipeline, puis les assembler
      *       - trouver les meilleurs hyperparamètres pour l'entraînement du pipeline avec une grid-search
      *       - Sauvegarder le pipeline entraîné
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/
    // chargement des données

    val df: DataFrame = spark
      .read
     // .parquet("/home/hdanlos/cours-spark-telecom/data/prepared_trainingset")
      .parquet("data/saved_data_with_launch")

    println(s"Nombre de lignes : ${df.count}")
    println(s"Nombre de colonnes : ${df.columns.length}")

    // Stage 1 : récupérer les mots des textes
    val tokenizer = new RegexTokenizer()
      .setPattern("\\W+")
      .setGaps(true)
      .setInputCol("text")
      .setOutputCol("tokens");

    // Stage 2 : retirer les stop words
    val remover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("filtered")

    // Stage 3 : calculer la partie TF
    val tf = new CountVectorizer()
        .setInputCol("filtered")
        .setOutputCol("tf")

    // Stage 4 : calculer la partie IDF
    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("tfidf")

    // Stage 5 : convertir country2 en quantités numériques
    val indexer = new StringIndexer()
      .setInputCol("country2")
      .setOutputCol("country_indexed")
      .setHandleInvalid("keep")

    // Stage 6 : convertir currency2 en quantités numériques
    val idxcur = new StringIndexer()
        .setInputCol("currency2")
        .setOutputCol("currency_indexed")

    // Stages 7 et 8: One-Hot encoder ces deux catégories
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("country_indexed", "currency_indexed"))
      .setOutputCols(Array("country_onehot", "currency_onehot"))

    // Stage 9 : assembler tous les features en un unique vecteur
    val assembler = new VectorAssembler()
      .setInputCols(Array("tfidf", "days_campaign", "hours_prepa", "goal",
        "country_onehot", "currency_onehot",
        "launch_day",
        //"launch_month",
        "launch_hour"))
      .setOutputCol("features")

    // Stage 10 : créer/instancier le modèle de classification
    val lr = new LogisticRegression()
      .setElasticNetParam(0.0)
      .setFitIntercept(true)
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setStandardization(true)
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")
      .setThresholds(Array(0.7, 0.3))
      .setTol(1.0e-6)
      .setMaxIter(20)

    println(s"Création du pipeline")

    // Création du pipeline
    val pipeline = new Pipeline()
      .setStages(Array(
        tokenizer,
        remover,
        tf,
        idf,
        indexer,
        idxcur,
        encoder,
        assembler,
        lr
      ))

    // Split des données en training et test sets
    val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 12345)

    // Entraînement du modèle
    val model = pipeline.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    //model.write.overwrite().save("/tmp/spark-logistic-regression-model")

    // Test du modèle
    val dfWithSimplePredictions = model.transform(test)
    dfWithSimplePredictions.groupBy("final_status", "predictions").count.show()

    val f1_eval = new MulticlassClassificationEvaluator()
    .setLabelCol("final_status")
    .setPredictionCol("predictions")

    val f1WithSimplePredictions = f1_eval.evaluate(dfWithSimplePredictions)

    println(f1WithSimplePredictions)

    println(s"grid search")

    // Grid search
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(10e-8, 10e-6, 10e-4,10e-2))
      .addGrid(lr.aggregationDepth,Array(2,5,10))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.fitIntercept,Array(false, true))
      .addGrid(tf.minDF, Array(55.0, 75.0, 95.0))
      .build()

    //
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(f1_eval)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)
    //  // Evaluate up to 2 parameter settings in parallel
    //  .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model_grid = trainValidationSplit.fit(training)

    val dfWithPredictions = model_grid.transform(test)

    dfWithPredictions.groupBy("final_status", "predictions").count.show()
    val f1WithPredictions = f1_eval.evaluate(dfWithPredictions)

    println(f1WithSimplePredictions, f1WithPredictions)

  }
}
