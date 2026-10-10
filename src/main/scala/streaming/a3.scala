package streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {def main(args: Array[String]): Unit = {
  if (args.length < 7) {
    System.err.println("Usage: Assignment3 <directory>")
    System.exit(1)
  }

  //StreamingExamples.setStreamingLogLevels()
  val sparkConf = new SparkConf().setAppName("Assignment3")
  // Creating the streaming context
  val ssc = new StreamingContext(sparkConf, Seconds(3))

  ssc.checkpoint("hdfs:///s4216579/checkpoint")

  // Create the FileInputDStream on the directory and use the
  // stream to count words in third column created (query text)
  val lines = ssc.textFileStream(args(0))
  val notProcessed = lines.map {line =>
    val columns = line.split(',')
    (columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))}

  val queryText = notProcessed.map{text => text._3}

  // Cleaning
  // https://www.baeldung.com/scala/string-delete-special-characters
  val stopWords = List("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
  def textCleaning(text: String): String = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9]", "")
      .split("\\s+").filter(_.nonEmpty).filter(_.length >=3)
      .filterNot(stopWords.contains).mkString(" ")}
  val cleaned = queryText.map(text => textCleaning(text))


  // Define the State Update Function
  val updateFunction = (newValues: Seq[Int], oldState: Option[Int]) => {
    val currentCount = newValues.sum
    val previousCount = oldState.getOrElse(0)
    Some(currentCount + previousCount)
  }

  cleaned.saveAsTextFiles("hdfs:///output")
  }
}
