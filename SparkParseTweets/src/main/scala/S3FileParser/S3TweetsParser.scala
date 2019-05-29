package S3FileParser

import org.apache.spark.sql.SparkSession
import java.util.{Calendar, Date}

object S3TweetsParser {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

    val BUCKET_NAME = "s3://simus-aws-assignment"
    val DIRECTORY_SOURCE = BUCKET_NAME + "/sparkS3connection/tweets/"
    val DIRECTORY_DESTINATION = BUCKET_NAME + "/sparkS3connection/tweetOutputDataframe/processing_date="

    val df = spark.read.json(DIRECTORY_SOURCE)

    //Showing some items from my Dataframe
    df.show
    df.select(df("created_at"), df("source"), df("text"), df("user.id"), df("user.name"), df("user.followers_count"), df("user.friends_count")).show()
    df.select(df("created_at")).show()


    val cal = Calendar.getInstance()
    val day =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1

    df.write.mode("append").format("json").save(DIRECTORY_DESTINATION + Year + Month + day)
  }
}
