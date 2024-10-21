package projekt.cleaner

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes

class TweetsCleaner(sparkSession: SparkSession) {

  def cleanAllTweets(df: Dataset[Row]): Dataset[Row]={
    df.withColumn("hashtags", regexp_replace(col("hashtags"), pattern = "[']", replacement = ""))
      .withColumn("hashtags", regexp_replace(col("hashtags"), pattern = "\\[", replacement = ""))
      .withColumn("hashtags", regexp_replace(col("hashtags"), pattern = "\\]", replacement = ""))
      .withColumn("hashtags", split(col("hashtags"), pattern = ","))
      .withColumn("date", col("date").cast(DataTypes.DateType))
      .withColumn("user_created", col("user_created").cast(DataTypes.DateType))
      .withColumn("user_favourites", col("user_favourites").cast(DataTypes.LongType))
      .withColumn("user_friends", col("user_friends").cast(DataTypes.LongType))
      .withColumn("user_followers", col("user_followers").cast(DataTypes.LongType))
  }

}
