package streaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {def main(args: Array[String]): Unit = {

  // Check the three required command-line arguments
  if (args.length < 3) {
    System.err.println("Usage: Assignment3 <input_directory><output_directory><query_threshold>")
    System.exit(1)
  }

  // Create the streaming context with a 3-second batch interval
  val sparkConf = new SparkConf().setAppName("Assignment3")
  val ssc = new StreamingContext(sparkConf, Seconds(3))
  ssc.checkpoint("hdfs:///s4216579/checkpoint")

  // Create the FileInputDStream on the directory
  val lines = ssc.textFileStream(args(0))
  val outputDir = args(1)
  val queryThresh = args(2).toFloat

  // Skip the header line in the csv
  val linesNoHeader = lines.filter(line =>
      !line.startsWith("query_id,user_id,query_text,event_time,device,result_count,response_time_ms"))
    .filter(line => line.split(",", -1).length >=7)

  // Cleaning function (Krishnan, 2024)
  val stopWords = List("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
  def textCleaning(text: String): String = {
    text.toLowerCase.replaceAll("[^a-zA-Z0-9 ]", "")
      .split(" ").filter(_.nonEmpty).filter(_.length >=3)
      .filterNot(stopWords.contains).mkString(" ")}

  // Sw tream to count cleaned queries in third column created (Sparking Scala, 2024)
  // Note: the index in split(..., -1) means it allows for some columns in this split to be empty
  val records = linesNoHeader.map {line => val columns = line.split(",", -1)
    (columns(0), columns(1),
      textCleaning(columns(2)), columns(3),
      columns(4), columns(5), columns(6))}


  // Create query text pairs
  val queryText = records.map{text => text._3}
  val pairs = queryText.map(x => (x, 1)).reduceByKey(_ + _)

  // Define the State Update Function
  val updateFunction = (newValues: Seq[Int], oldState: Option[(Int, Int, Int, Int, String)]) => {
    val current_batch_count = newValues.sum
    val previous_accumulated_count = oldState.map(_._3).getOrElse(0)
    val accumulated_count = previous_accumulated_count + current_batch_count
    val count_change = accumulated_count - previous_accumulated_count
    val trend_status =
      if (oldState.isEmpty) {
        "NEW"
      } else if (count_change > 0){
        "INCREASED"
      } else {
        "UNCHANGED"
      }
    Some((current_batch_count, previous_accumulated_count, accumulated_count, count_change, trend_status))
  }
  val accumulatedStatistics = pairs.updateStateByKey[(Int, Int, Int, Int, String)](updateFunction)


  // Task saves for non-empty batches (Thakur, 2019)
  var batchNumber = 1
  var emptyBatch = true

  records.foreachRDD {Rdd =>
    emptyBatch = Rdd.isEmpty()

    if (!emptyBatch){
      // Task 1 - sorting the word counts (Landei, 2011)
      val sortedCounts = Rdd.map(x => (x._3, 1)).reduceByKey(_ + _)
        .sortBy{case (word, count) => (-count, word)}
        // remove brackets by making it into a string (Scala, 2025)
        .map(_.productIterator.mkString(","))


      // Task 2
      // Creating a composite key
      val compositeUserQuery = Rdd.map{case (_, user_id, query_text, _, _, _, response_time) =>
        val time = response_time.toLong
        val slow_query_count = if (time > queryThresh) 1L else 0L
        ((query_text, user_id), (1L,  time, time, slow_query_count))}
      // aggregating for composite key a and b
      val aggregatedCompositeValues = compositeUserQuery.reduceByKey{ (a, b) =>
        (a._1 + b._1,       // Query frequency
          a._2 + b._2,      // total response time
          a._3.max(b._3),   // Max response time
          a._4 + b._4       // slow query count
        )}
      // Change back to the query key
      val byQuery = aggregatedCompositeValues.map{
        case ((query_text, user_id), (frequency, time_total, max_time, slow_query_count)) =>
        val affected_users = if (slow_query_count > 0) 1L else 0L

        (query_text, (frequency, time_total, max_time, slow_query_count, affected_users))}

      // Aggregating by Query
      val aggregatedQuery = byQuery.reduceByKey { (a, b) =>
        (a._1 + b._1,     // query frequency
          a._2 + b._2,    // total response time
          a._3.max(b._3), // max response time
          a._4 + b._4,    // slow query count
          a._5 + b._5     // affected user count
        )}

      val resultsTask2 = {
        // Filtering only
        aggregatedQuery.filter{
          case(_, (_, _, _, slow_query_count, _)) => slow_query_count > 0
        }.map{ case
          (cleaned_query, (query_frequency, time_total, max_response_time, slow_query_count, affected_users)) =>
          val average_response_time_unformatted = time_total.toDouble / query_frequency
          // 2 decimal place format (Rijo, 2024)
          val average_response_time_ms = f"$average_response_time_unformatted%.2f"
          val severity_level  = if (max_response_time >= queryThresh * 2) "CRITICAL"
          else "WARNING"

            (
              cleaned_query,
              slow_query_count,
              max_response_time,
              average_response_time_ms,
              affected_users,
              severity_level
            )
      //Applying sorting
      }.sortBy{case
          (cleaned_query, slow_query_count, max_response_time, _, _, severity_level) =>
          (severity_level, -max_response_time, -slow_query_count, cleaned_query)}
      }.map(_.productIterator.mkString(","))



      // Save task 1
      sortedCounts.saveAsTextFile(f"$outputDir/task1-$batchNumber%03d")
      // Save task 2
      resultsTask2.saveAsTextFile(f"$outputDir/task2-$batchNumber%03d")
    }
  }

  // Task 3
  accumulatedStatistics.foreachRDD(Rdd =>
    if (!emptyBatch){
      // maps (to remove from tuple) and then sorts
      val sortedStatistics = Rdd.map{case (query, (a, b, c, d, e)) => (query, a, b, c, d, e)}
        .sortBy{case (query, current_batch_count, previous_accumulated_count, accumulated_count, count_change, trend_status)
        => ( -accumulated_count, -current_batch_count, query)}.take(10)

      Rdd.sparkContext.parallelize(sortedStatistics.toSeq, 1)
        .map(_.productIterator.mkString(","))
        .saveAsTextFile(f"$outputDir/task3-$batchNumber%03d")
      batchNumber += 1
    })

  ssc.start()
  ssc.awaitTermination()
}
}

// References
// Krishnan, Y. (2024, April 25). Remove special characters from a string in Scala. Baeldung on Scala. https://www.baeldung.com/scala/string-delete-special-characters
// Landei. (2011, January 25). Scala map sorting. Stack Overflow. https://stackoverflow.com/questions/4793490/scala-map-sorting
// Rijo, P. (2024, September 4). Different ways to format numbers in Scala. Baeldung on Scala. https://www.baeldung.com/scala/number-formats
// Scala. (2025). Iterators. Scala Documentation. https://docs.scala-lang.org/overviews/collections/iterators.html
// Sparking Scala. (2024). Split function: Split strings into arrays in spark scala DataFrames. Sparkingscala.Com. https://sparkingscala.com/examples/split/
// Thakur, S. (2019, August 30). In Spark Streaming, how to detect for an empty batch? . Stack Overflow. https://stackoverflow.com/questions/29134612/in-spark-streaming-how-to-detect-for-an-empty-batch