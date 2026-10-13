package streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {def main(args: Array[String]): Unit = {
  if (args.length < 3) {
    System.err.println("Usage: Assignment3 <input_directory><output_directory><query_threshold>")
    System.exit(1)
  }

  //StreamingExamples.setStreamingLogLevels()
  val sparkConf = new SparkConf().setAppName("Assignment3")
  // Creating the streaming context
  val ssc = new StreamingContext(sparkConf, Seconds(3))

  ssc.checkpoint("hdfs:///s4216579/checkpoint")

  // Create the FileInputDStream on the directory
  val lines = ssc.textFileStream(args(0))
  val outputDir = args(1)
  val queryThresh = args(2).toFloat

  // Skip the header line in the csv
  val linesNoHeader = lines.filter(line =>
    !line.startsWith("query_id,user_id,query_text,event_time,device,result_count,response_time_ms"))

  // Cleaning function
  // https://www.baeldung.com/scala/string-delete-special-characters
  val stopWords = List("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
  def textCleaning(text: String): String = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9 ]", "")
      .split(" ").filter(_.nonEmpty).filter(_.length >=3)
      .filterNot(stopWords.contains).mkString(" ")}

  // stream to count cleaned queries in third column created (query text)
  val records = linesNoHeader.map {line => val columns = line.split(',')
    (columns(0), columns(1),
      textCleaning(columns(2)), columns(3),
      columns(4), columns(5), columns(6))}


  // Create query text pairs
  val queryText = records.map{text => text._3}
  val pairs = queryText.map(x => (x, 1)).reduceByKey(_ + _)

  // Define the State Update Function
  val updateFunction = (newValues: Seq[Int], oldState: Option[Int]) => {
    val currentCount = newValues.sum
    val previousCount = oldState.getOrElse(0)
    Some(currentCount + previousCount)
  }
  val queryCounts = pairs.updateStateByKey(updateFunction)


  // Task saves for non-empty batches
  // ref https://stackoverflow.com/questions/29134612/in-spark-streaming-how-to-detect-for-an-empty-batch
  var batchNumber = 1

  records.foreachRDD {Rdd =>
    if (!Rdd.isEmpty()){
      val currentRdd = Rdd.cache()
      // Task 1 - sorting the word counts
      // Landei https://stackoverflow.com/questions/4793490/scala-map-sorting
      val sortedCounts = currentRdd.map(x => (x._3, 1)).reduceByKey(_ + _)
        .sortBy{case (word, count) => (-count, word)}
      // Task 2
      val slowQuery = currentRdd.filter(x => x._7.toFloat > queryThresh)

      // Save task 1
      sortedCounts.saveAsTextFile(f"$outputDir/task1-$batchNumber%03d")
      // Save task 3
      currentRdd.saveAsTextFile(f"$outputDir/task3-$batchNumber%03d")
      batchNumber += 1
      currentRdd.unpersist(false)
      }
    }
  queryCounts.print()
  ssc.start()
  ssc.awaitTermination()
  }
}
