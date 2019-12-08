package paristech

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.datediff
import org.apache.spark.sql.functions.dayofweek
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.hour
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.round
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.concat_ws

object Preprocessor {

  def main(args: Array[String]): Unit = {

    // Des réglages optionnels du job spark. Les réglages par défaut fonctionnent très bien pour ce TP.
    // On vous donne un exemple de setting quand même
    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12"
    ))

    // Initialisation du SparkSession qui est le point d'entrée vers Spark SQL (donne accès aux dataframes, aux RDD,
    // création de tables temporaires, etc., et donc aux mécanismes de distribution des calculs)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP Spark : Preprocessor")
      .getOrCreate()

    // import in order to be able to use the $ notation
    // NB : this import must be done *after* the creation of the spark session !
    import spark.implicits._
    /*******************************************************************************
      *
      *       TP 2
      *
      *       - Charger un fichier csv dans un dataFrame
      *       - Pre-processing: cleaning, filters, feature engineering => filter, select, drop, na.fill, join, udf, distinct, count, describe, collect
      *       - Sauver le dataframe au format parquet
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/



    // chargement des données

    val df: DataFrame = spark
      .read
      .option("header", true) // utilise la première ligne du (des) fichier(s) comme header
      .option("inferSchema", "true") // pour inférer le type de chaque colonne (Int, String, etc.)
      .csv("data/train_clean.csv")

    println(s"Nombre de lignes : ${df.count}")
    println(s"Nombre de colonnes : ${df.columns.length}")

    //df.show()
    //df.printSchema()

    // cast des colonnes contenant des entiers

    val dfCasted: DataFrame = df
      .withColumn("goal", $"goal".cast("Int"))
      .withColumn("deadline" , $"deadline".cast("Int"))
      .withColumn("state_changed_at", $"state_changed_at".cast("Int"))
      .withColumn("created_at", $"created_at".cast("Int"))
      .withColumn("launched_at", $"launched_at".cast("Int"))
      .withColumn("backers_count", $"backers_count".cast("Int"))
      .withColumn("final_status", $"final_status".cast("Int"))

    //dfCasted.printSchema()

    // nettoyage des données

    /*dfCasted
        .select("goal","backers_count", "final_status")
        .describe()
        .show()*/

    //dfCasted.groupBy("disable_communication").count.orderBy($"count".desc).show(100)

    // content of disable_communication is meaningless, remove
    val df2: DataFrame = dfCasted.drop("disable_communication")

    //dfCasted.groupBy("state_changed_at").count.orderBy($"count".desc).show(100)
    //dfCasted.groupBy("backers_count").count.orderBy($"count".desc).show(100)

    // state_changed_at and backers_count are leaks from the future, remove
    val dfNoFuture: DataFrame = df2.drop("backers_count", "state_changed_at")

    //dfCasted.groupBy("country").count.orderBy($"count".desc).show(100)
    //dfCasted.groupBy("currency").count.orderBy($"count".desc).show(100)
    //dfCasted.groupBy("country", "currency").count.orderBy($"count".desc).show(50)
    /*df.filter($"country" === "False")
      .groupBy("currency")
      .count
      .orderBy($"count".desc)
      .show(50) */

    // when the value of the country is false, use the one from the currency column instead
    def cleanCountry(country: String, currency: String): String = {
      if (country == "False" || country == "True")
        currency
      else
        country
    }

    // currency codes must be 3 characters long
    def cleanCurrency(currency: String): String = {
      if (currency != null && currency.length != 3)
        null
      else
        currency
    }

    val cleanCountryUdf = udf(cleanCountry _)
    val cleanCurrencyUdf = udf(cleanCurrency _)

    val dfCountry: DataFrame = dfNoFuture
      .withColumn("country2", cleanCountryUdf($"country", $"currency"))
      .withColumn("currency2", cleanCurrencyUdf($"currency"))
      .drop("country", "currency")


    //dfCasted.select("deadline").dropDuplicates.show()
    dfCasted.select("goal", "final_status").show(30)

    //dfCountry.show()
    //dfCountry.printSchema()

    // montrer les différentes classes cibles représentées
    dfCountry.groupBy("final_status").count.orderBy($"count".desc).show()

    val dfCible : DataFrame = dfCountry.filter($"final_status" <= 1)

    println(s"Nombre de lignes : ${dfCible.count}")
    println(s"Nombre de colonnes : ${dfCible.columns.length}")
    dfCible.groupBy("final_status").count.orderBy($"count".desc).show()

    val dfNew : DataFrame = dfCible
      .withColumn("days_campaign", datediff(from_unixtime($"deadline"),from_unixtime($"launched_at")))
      .withColumn("hours_prepa", round(($"launched_at" - $"created_at")/3600,3))
      .withColumn("name", lower($"name"))
      .withColumn("desc",lower($"desc"))
      .withColumn("keywords",lower($"keywords"))
      .withColumn("text",concat_ws(" ",$"name",$"desc",$"keywords"))
      .withColumn("launch_day", dayofweek(from_unixtime($"launched_at")))
      .withColumn("launch_month", month(from_unixtime($"launched_at")))
      .withColumn("launch_hour", hour(from_unixtime($"launched_at")))
      .drop("deadline","launched_at","created_at")

    dfNew.show()
    dfNew.printSchema()

    dfNew.groupBy("days_campaign").count.orderBy($"count".desc).show()
    dfNew.groupBy("hours_prepa").count.orderBy($"count".desc).show()
    dfNew.groupBy("goal").count.orderBy($"count".desc).show()
    dfNew.groupBy("country2").count.orderBy($"count".desc).show()
    dfNew.groupBy("currency2").count.orderBy($"count".desc).show()
    dfNew.groupBy("launch_day").count.orderBy($"count".desc).show()
    dfNew.groupBy("launch_month").count.orderBy($"count".desc).show()
    dfNew.groupBy("launch_hour").count.orderBy($"count".desc).show()

    val dfNext : DataFrame = dfNew
      .na.fill(-1, Seq("days_campaign","hours_prepa","goal"))
      .na.fill("unknown",Seq("country2","currency2"))

    dfNext.summary().show()
    dfNext.show()

    dfNext.write.parquet("data/preprocessed")

    println("Hello World ! from Preprocessor")
    println("\n")
  }
}
