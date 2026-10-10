package streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {def main(args: Array[String]): Unit = {
  if (args.length < 1) {
    System.err.println("Usage: Assignment3 <directory>")
    System.exit(1)
  }

  //StreamingExamples.setStreamingLogLevels()
  val sparkConf = new SparkConf().setAppName("Assignment3")
  // Creating the streaming context
  val ssc = new StreamingContext(sparkConf, Seconds(3))

  ssc.checkpoint("hdfs:///s4216579/checkpoint")

  // Create the FileInputDStream on the directory
  val lines = ssc.textFileStream(args(0))

  // Cleaning function which is used for creating records
  // https://www.baeldung.com/scala/string-delete-special-characters
  val stopWords = List("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
  def textCleaning(text: String): String = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9 ]", "")
      .split(" ").filter(_.nonEmpty).filter(_.length >=3)
      .filterNot(stopWords.contains).mkString(" ")}

  // stream to count words in third column created (query text)
  val records = lines.map {line => val columns = line.split(',')
    (columns(0), columns(1), textCleaning(columns(2)),
      columns(3),
      columns(4), columns(5), columns(6))}

  // val queryText = records.map{text => text._3}

  // Define the State Update Function
  val updateFunction = (newValues: Seq[Int], oldState: Option[Int]) => {
    val currentCount = newValues.sum
    val previousCount = oldState.getOrElse(0)
    Some(currentCount + previousCount)
  }

  // Create query text pairs
  val queryText = records.map{text => text._3}
  val pairs = queryText.map(x => (x, 1)).reduceByKey(_ + _)
  val wordCounts = pairs.updateStateByKey(updateFunction)


  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()

  records.saveAsTextFiles("hdfs:///output")
  }
}
