package projekt.analizer

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object TweetsAnalyzer{
  val HASHTAG_COLUMN: String = "hashtag"
  val IS_RETWEET_COLUMN: String = "is_retweet"
  val SOURCE_COLUMN: String = "Source"
  val USER_FOLLOWERS: String = "user_followers"
  val USER_NAME: String = "user_name"
  val USER_LOCATION: String = "user_location"
}
class TweetsAnalyzer(sparkSession: SparkSession) {
  /**
   * AGREGATION
   * @param df
   * @return Dataframe with colymns hashtag, count
   */
  def calculateHashtags(df: Dataset[Row]): Dataset[Row]={
    df.withColumn(TweetsAnalyzer.HASHTAG_COLUMN, explode_outer(col(TweetsAnalyzer.HASHTAG_COLUMN)))
      .groupBy(TweetsAnalyzer.HASHTAG_COLUMN).count()
  }

  /**
   * AGREGATION
   * @param df
   * @return Dataframe with colymns is_retweet, count
   */
  def calculateIsRetweetCount(df: Dataset[Row]): Dataset[Row]={
    df.groupBy(TweetsAnalyzer.IS_RETWEET_COLUMN).count()
  }

  /**
   * AGREGATION
   * @param df
   * @return Dataframe with colymns source, count
   */
  def calculateSourceCount(df: Dataset[Row]): Dataset[Row]={
    df.groupBy(TweetsAnalyzer.SOURCE_COLUMN).count()
  }

  /**
   * AGREGATION
   * @param df
   * @return Dataframe with colymns USER_LOCATION, avg
   */
  def calculateAvgUserFollowersPerLocation(df: Dataset[Row]): Dataset[Row]={
    df.select(TweetsAnalyzer.USER_NAME, TweetsAnalyzer.USER_FOLLOWERS, TweetsAnalyzer.USER_LOCATION)
      .filter(col(TweetsAnalyzer.USER_NAME).isNotNull)
      .filter(col(TweetsAnalyzer.USER_LOCATION).isNotNull)
      .dropDuplicates(TweetsAnalyzer.USER_NAME)
      .groupBy(TweetsAnalyzer.USER_LOCATION)
      .avg(TweetsAnalyzer.USER_FOLLOWERS)
      .as("avg")
  }
}
