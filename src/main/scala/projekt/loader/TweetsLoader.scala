package projekt.loader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lit

object TweetsLoader{
  val COVID_LABEL: String = "covid"
  val GRAMMYS_LABEL: String = "grammys"
  val FINANCE_LABEL: String = "finance"
}
class TweetsLoader(sparkSession: SparkSession) {

  def loadAllTweets(): Dataset[Row]= {
    val covidDF: Dataset[Row] = loadCovid()
    val financialDF: Dataset[Row] = loadFinancial()
    val grammysDF: Dataset[Row] = loadGrammys()

    covidDF.unionByName(financialDF, allowMissingColumns = true)
      .unionByName(grammysDF, allowMissingColumns = true)
  }

  def loadCovid(): Dataset[Row]={
      sparkSession.read
        .option("header", "true")
        .csv("covid19_tweets.csv")
        .withColumn("category", lit(TweetsLoader.COVID_LABEL))
        .na.drop()
  }

  def loadFinancial(): Dataset[Row]={
      sparkSession.read
        .option("header", "true")
        .csv("financial.csv")
        .withColumn("category", lit(TweetsLoader.FINANCE_LABEL))
        .na.drop()
  }

  def loadGrammys(): Dataset[Row]={
      sparkSession.read
        .option("header", "true")
        .csv("GRAMMYs_tweets.csv")
        .withColumn("category", lit(TweetsLoader.GRAMMYS_LABEL))
        .na.drop()
  }
}
