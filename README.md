# Task documentation and requirements 
Note:

For this task only output the accumulated query statistics when the monitored HDFS directory receives new query log files. Namely, if no new query log files arrive during an interval, produce no output.


## Text processing: 
Before performing the following tasks, each query_text must be cleaned and normalised:
1. Case folding: convert text to lowercase.
2. Remove non-alphanumeric characters.
3. Remove extra spaces.
4. Split the query into terms.
5. Remove terms shorter than 3 characters.
6. Remove common stop words: the, and, for, with, how, what, why, can, you, use, using
7. Reconstruct the cleaned query text

Note: Records with an empty cleaned query must be ignored.

## Task 1 – Cleaned Query Frequency Analysis
For each non-empty RDD in the DStream, count how many times each cleaned query appears in the current batch. The output must be

cleaned_query,current_batch_count 

The output must be sorted by current_batch_count in descending order, and then by cleaned_query in ascending alphabetical order. This analysis provides a snapshot of user search activity within the current streaming batch. By counting the frequency of each cleaned query, organisations can identify which topics are attracting the most attention at a particular point in time.
Only non-empty RDDs should generate output. Save the output on HDFS.

## Task 2 - Slow Query Incident Monitoring 

Slow queries can negatively affect user experience, reduce system responsiveness, and indicate potential
performance bottlenecks in the underlying search infrastructure. By analysing slow-query incidents in real time,
organisations can identify problematic search terms, monitor the severity of performance issues, and understand
how many users are affected.
For each RDD of Dstream, identify slow queries and produce a summary for each cleaned query in the current
batch. A query is considered slow if its response_time_ms is greater than a specified threshold. The
threshold must be provided as a command-line argument when the Spark Streaming application is executed.

For each cleaned query, calculate the following values:
* slow_query_count indicates how frequently the slow query occurred in the current batch.
* max_response_time_ms highlights the worst response time observed for the query.
* average_response_time_ms provides an overall measure of the query's performance.
* affected_users indicates how many distinct users experienced the slow query and therefore reflects
the impact of the issue.
severity_level classifies the incident according to its seriousness, helping system administrators
prioritise investigation and remediation efforts. The severity_level should be assigned as follows:

CRITICAL if max_response_time_ms >= threshold * 2
WARNING otherwise

The output must be:

cleaned_query,slow_query_count,max_response_time_ms,average_response_time_ms,affected_users,severity_level

The output should be sorted by:

severity_level ASC, max_response_time_ms DESC, slow_query_count DESC, cleaned_query ASC

This ordering helps system administrators identify and prioritise the most critical performance issues.
1. Queries with a CRITICAL severity level are displayed before those classified as WARNING, ensuring that the most serious incidents receive immediate attention.
2. Within the same severity level, queries with higher max_response_time_ms are ranked higher because they represent the most severe performance degradation.
3. If multiple queries have the same severity and maximum response time, queries with a higher slow_query_count are prioritised because they indicate more frequent occurrences of the problem.
4. Finally, cleaned_query is sorted alphabetically to provide a consistent and reproducible ordering when all other ranking criteria are identical. This prioritisation approach is commonly used in operational monitoring systems because it allows engineers to focus on the most critical and impactful performance problems before investigating less severe incidents. Only non-empty RDDs should generate output. Save the output on HDFS.

## Task 3 - Advanced Stateful Trending Query Analysis 
Using updateStateByKey, maintain accumulated query statistics across all batches. For each cleaned query, maintain:

current_batch_count
previous_accumulated_count
accumulated_count
count_change
trend_status
where count_change = accumulated_count - previous_accumulated_count

The trend_status should be:
NEW
INCREASED
UNCHANGED
A query is NEW if it appears for the first time, INCREASED if its accumulated count increases in the current batch, and UNCHANGED if it already exists but does not increase in the current batch.
For each valid batch, output the top 10 most frequent queries based on accumulated statistics. The output
should contain:

cleaned_query,current_batch_count,previous_accumulated_count,accumulated_count,count_chang
e,trend_status

Ranking rules for the top 10 trending queries:
1. Higher accumulated_count ranks higher. accumulated_count measures the total number of times a query has appeared since the streaming application started. Queries with higher accumulated counts represent long-term trends and therefore receive higher priority.
2. If tied, higher current_batch_count ranks higher. current_batch_count is used when multiple queries have the same accumulated count. This favours queries that are currently experiencing higher activity and helps identify emerging trends among queries with similar historical popularity.
3. If still tied, sort by cleaned_query in ascending alphabetical order. cleaned_query in ascending alphabetical order, provides a deterministic ordering when all numerical measures are identical. This ensures consistent and reproducible output across executions. Together, the ranking provides a balanced view of both historical popularity and recent user activity, enabling organisations to identify sustained trends, emerging interests, and short-lived search patterns within the query stream. This approach is commonly used in streaming analytics systems where both historical popularity and current activity are important indicators of user interest. 

Note: you are required to use updateStateByKey to continuously accumulative query statistics as new query log arrives. 
