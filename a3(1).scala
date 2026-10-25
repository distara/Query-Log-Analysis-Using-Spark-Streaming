package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {
  def main(args: Array[String]): Unit = {

    // Check the three required command-line arguments
    if (args.length != 3) {
      System.err.println("Usage: a3 <input_directory> <output_directory> <query_threshold>")
      System.exit(1)
    }

    // Create the streaming context with a 3-second batch interval
    val sparkConf = new SparkConf().setAppName("a3")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("hdfs:///s4204060/checkpoint")
    // Create FileInputDStream
    val inputDir = args(0)
    val outputDir = args(1)
    val threshold = args(2).toLong
    val lines = ssc.textFileStream(inputDir)

    // Clean search query
    def clean(query: String): String = {
      // Stop words specified in the assignment
      val stopWords = Set("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
      // Convert to lowercase and remove non-alphanumeric characters
      val lowercase = query.toLowerCase
      val alphanumeric = lowercase.replaceAll("[^a-z0-9]", " ")
      // Remove extra spaces
      val rmspace = alphanumeric.trim.replaceAll("\\s+", " ")
      // Split into terms
      val terms = rmspace.split("\\s+")
        .filter(word => word.length >= 3)
        .filter(word => !stopWords.contains(word))
      // Reconstruct the cleaned query
      terms.mkString(" ")
    }
    // Extract and clean queries from CSV records
    val records = lines
      .map(line => line.split(",", -1))
      .filter(columns => columns.length == 7)
      .filter(columns => columns(0) != "query_id")
      .filter(columns => columns(6).trim.matches("[0-9]+"))
      .map(columns => (clean(columns(2)), columns(1).trim, columns(6).trim.toLong))
      .filter(record => record._1.nonEmpty)
    val cleanQuery = records.map(record => record._1)

    // Task1
    val queryCounts = cleanQuery
      .map(query => (query, 1))
      .reduceByKey(_ + _)
    // Sort and save results
    var task1Number = 1
    queryCounts.foreachRDD { rdd =>
      // Save when the batch only contains cleaned queries
      if (!rdd.isEmpty()) {
        // Sort by count in descending order, then query alphabetically
        val sortCounts = rdd.sortBy({case (query, count) => (-count, query) }, ascending = true)
        val output = sortCounts.map {case (query, count) => s"$query,$count"}
        // Save non-empty batch to a numbered HDFS directory
        val outputPath = f"$outputDir/task1-$task1Number%03d"
        output.saveAsTextFile(outputPath)
        task1Number += 1
      }
    }

    // Task2
    val slowQueries = records
      .filter(record => record._3 > threshold)
    // Group slow records by cleaned query
    val slowGroups = slowQueries
      .map {case (query, user, responseTime) => (query, (user, responseTime))
      }
      .groupByKey()
    // Calculate statistics for each cleaned query
    val slowStats = slowGroups.map {case (query, values) =>
        val data = values.toList
        val times = data.map(_._2)
        val slowCount = data.size
        val maxTime = times.max
        val avgTime = times.map(_.toDouble).sum / slowCount
        val affectedUsers = data.map(_._1).distinct.size
        val severity =
          if (maxTime >= threshold * 2) "CRITICAL"
          else "WARNING"
        (query, slowCount, maxTime, avgTime,
          affectedUsers, severity)
    }
    var task2Number = 1
    slowStats.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        // Sort by severity, maximum time, count, and query
        val sortSlow = rdd.sortBy(record => (record._6, -record._3, -record._2, record._1), ascending = true)
        val output = sortSlow.map {case (query, count, maxTime, avgTime, users, severity) =>
          f"$query,$count,$maxTime,$avgTime%.2f,$users,$severity"}
        val outputPath = f"$outputDir/task2-$task2Number%03d"
        output.saveAsTextFile(outputPath)
        task2Number += 1
      }
    }

    // Task3
    val accumulatedStatistics = queryCounts.updateStateByKey[
      (Int, Int, Int, Int, String)] { (counts: Seq[Int], previousState: Option[(Int, Int, Int, Int, String)]) =>
        val currentCount = counts.sum
        previousState match {
          // New query
          case None =>
            Some((currentCount, 0, currentCount, currentCount, "NEW"))
            // Query already exists
          case Some((_, _, oldTotal, _, _)) =>
            val newTotal = oldTotal + currentCount
            val status =
              if (currentCount > 0) "INCREASED"
              else "UNCHANGED"
            Some((currentCount, oldTotal, newTotal, currentCount, status))
        }
    }
    var task3Number = 1
    accumulatedStatistics.foreachRDD { rdd =>
      // Skip empty batches
      if (!rdd.filter { case (query, state) => state._1 > 0 }.isEmpty()) {
        // Sort all accumulated queries including unchanged ones
        val sortTrends = rdd.sortBy(
          { case (query, state) => (-state._3, -state._1, query)}, ascending = true)
        // Take the top 10 queries
        val top10 = sortTrends.take(10)
        val output = rdd.sparkContext
          .parallelize(top10.toSeq, 1)
          .map {
            case (query, state) =>
              val (current, previous, accumulated, change, status) = state
              s"$query,$current,$previous,$accumulated,$change,$status"}
        // Save
        val outputPath = f"$outputDir/task3-$task3Number%03d"
        output.saveAsTextFile(outputPath)
        task3Number += 1
      }
    }

    // Start streaming
    ssc.start()
    ssc.awaitTermination()
  }
}