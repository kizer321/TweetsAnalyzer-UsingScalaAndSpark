package projekt

import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import projekt.analizer.{TweetsAnalyzer, TweetsSearch}
import projekt.cleaner.TweetsCleaner
import projekt.loader.TweetsLoader

/**
 * 0. jak wygladaja dane, jakie sa pozorne typy, jak wyglada schemat
 * 1. ladowanie plików do sparka
 * 2. cleaning
 * 3. przeszukiwanie i analiza
 */

object TwiterApp {
  def main(args: Array[String]): Unit={
    val spark: SparkSession = SparkSession.builder()
      .appName("fundament sparka")
      .master("local[*]") // mozemy okreslic liczbe corów które bedą uzyte do zadania * to wszystkie 
      .getOrCreate()

    val tweetsLoader: TweetsLoader = new TweetsLoader(spark)
    val tweetsCleaner: TweetsCleaner = new TweetsCleaner(spark)
    val tweetsSearch: TweetsSearch = new TweetsSearch(spark)
    val tweetsAnalyzer: TweetsAnalyzer = new TweetsAnalyzer(spark)

    import tweetsSearch._

    val tweetsDF: Dataset[Row] = tweetsLoader.loadAllTweets().cache()
    val tweetsCleanedDF: Dataset[Row] = tweetsCleaner.cleanAllTweets(tweetsDF)

    val trumpTweetsDFWholeWorld: Dataset[Row] = tweetsCleanedDF.transform(searchByKeyWord("Trump"))

    val trumpTweetsDF: Dataset[Row] = tweetsCleanedDF.transform(searchByKeyWord("Trump"))
      .transform(onlyInLocation("United States"))

    val covidTweetsDF: Dataset[Row] = tweetsCleanedDF.transform(searchByKeyWord("Covid"))

    //covidTweetsDF.show()

    val covidCountDF: Dataset[Row] = tweetsAnalyzer.calculateSourceCount(covidTweetsDF)

    //covidCountDF.show()
    //covidTweetsDF.printSchema()
    val covidCountryDF: Dataset[Row] = covidTweetsDF.groupBy("user_location").count()
    val covidCountSortedDF: Dataset[Row] = covidCountryDF.orderBy(desc("count"))

    covidCountSortedDF.show()

    val sourceCount1: Dataset[Row] = tweetsAnalyzer.calculateSourceCount(trumpTweetsDFWholeWorld)
    val sourceCount2: Dataset[Row] = tweetsAnalyzer.calculateSourceCount(trumpTweetsDF)


   // sourceCount1.show()
    //sourceCount2.show()

  }
}
