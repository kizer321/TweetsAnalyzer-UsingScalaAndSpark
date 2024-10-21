package projekt.analizer

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object TweetsSearch{
  val TEXT: String = "text"
  val USER_LOCATION = "user_location"
}
class TweetsSearch(sparkSession: SparkSession) {

  def searchByKeyWord(keyWord: String)(df: Dataset[Row]): Dataset[Row]={
    df.filter(col(TweetsSearch.TEXT).contains(keyWord))
  }

  def searchByKeyWords(keyWords: Seq[String])(df: Dataset[Row]): Dataset[Row]={
    df.withColumn("keyWordsResult", array_intersect(split(col(TweetsSearch.TEXT), pattern = " "), split(lit(keyWords.mkString(",")), pattern = ",")))
      .filter(!(col("keyWordsResult").isNull.or(size(col("keyWordsResult")).equalTo(0))))
      .drop("keyWordsResult")
  }

  def onlyInLocation(location: String)(df: Dataset[Row]): Dataset[Row]={
    df.filter(col(TweetsSearch.USER_LOCATION).equalTo(location))
  }
}
