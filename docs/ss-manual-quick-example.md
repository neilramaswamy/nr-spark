---
layout: global
title: A Quick Example
displayTitle: A Quick Example
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

FYI: I think this example is way too complicated, mostly because the socket sink is too complicated. Why should a "quick example" require you to run netcat? Not great IMO. But we definitely can improve it -- leaving this comment here for just that reason (TODO) :)

Let’s say you want to maintain a running word count of text data received from a data server listening on a TCP socket. Let’s see how you can express this using Structured Streaming. You can see the full code in
[Python]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/python/sql/streaming/structured_network_wordcount.py)/[Scala]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/scala/org/apache/spark/examples/sql/streaming/StructuredNetworkWordCount.scala)/[Java]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/java/org/apache/spark/examples/sql/streaming/JavaStructuredNetworkWordCount.java)/[R]({{site.SPARK_GITHUB_URL}}/blob/v{{site.SPARK_VERSION_SHORT}}/examples/src/main/r/streaming/structured_network_wordcount.R).
And if you [download Spark](https://spark.apache.org/downloads.html), you can directly [run the example](index.html#running-the-examples-and-shell). In any case, let’s walk through the example step-by-step and understand how it works. First, we have to import the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.

<div class="codetabs">

<div data-lang="python"  markdown="1">

{% highlight python %}
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
 .builder \
 .appName("StructuredNetworkWordCount") \
 .getOrCreate()
{% endhighlight %}

</div>

<div data-lang="scala"  markdown="1">

{% highlight scala %}
import org.apache.spark.sql.functions.\_
import org.apache.spark.sql.SparkSession

val spark = SparkSession
.builder
.appName("StructuredNetworkWordCount")
.getOrCreate()

import spark.implicits.\_
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.\*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

SparkSession spark = SparkSession
.builder()
.appName("JavaStructuredNetworkWordCount")
.getOrCreate();
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
sparkR.session(appName = "StructuredNetworkWordCount")
{% endhighlight %}

</div>
</div>

Next, let’s create a streaming DataFrame that represents text data received from a server listening on localhost:9999, and transform the DataFrame to calculate word counts.

<div class="codetabs">

<div data-lang="python"  markdown="1">

{% highlight python %}

# lines is a streaming DataFrame representing the stream of input lines from localhost:9999

lines = spark \
 .readStream \

# In the future, you'll be able to use "kafka", "delta", etc.

.format("socket") \

# Every source stream has its own configuration options.

# For the "socket" source, it's "host" and "port".

.option("host", "localhost") \
 .option("port", 9999) \

# .load() completes the source configuration process

.load()

# Over a streaming DataFrame, you can use Spark syntax!

# Explode every line, i.e. create one row per word

words = lines.select(
explode(
split(lines.value, " ")
).alias("word")
)

# Group by the "word" column we just created

# After grouping, .count() "reduces" the group into its size

wordCounts = words.groupBy("word").count()
{% endhighlight %}

This `lines` DataFrame represents an unbounded table containing the streaming text data. This table contains one column of strings named "value", and each line in the streaming text data becomes a row in the table. Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it. Next, we have used two built-in SQL functions - split and explode, to split each line into multiple rows with a word each. In addition, we use the function `alias` to name the new column as "word". Finally, we have defined the `wordCounts` DataFrame by grouping by the unique values in the Dataset and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.

</div>

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// Create DataFrame representing the stream of input lines from connection to localhost:9999
val lines = spark.readStream
.format("socket")
.option("host", "localhost")
.option("port", 9999)
.load()

// Split the lines into words
val words = lines.as[String].flatMap(\_.split(" "))

// Generate running word count
val wordCounts = words.groupBy("value").count()
{% endhighlight %}

This `lines` DataFrame represents an unbounded table containing the streaming text data. This table contains one column of strings named "value", and each line in the streaming text data becomes a row in the table. Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it. Next, we have converted the DataFrame to a Dataset of String using `.as[String]`, so that we can apply the `flatMap` operation to split each line into multiple words. The resultant `words` Dataset contains all the words. Finally, we have defined the `wordCounts` DataFrame by grouping by the unique values in the Dataset and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// Create DataFrame representing the stream of input lines from connection to localhost:9999
Dataset<Row> lines = spark
.readStream()
.format("socket")
.option("host", "localhost")
.option("port", 9999)
.load();

// Split the lines into words
Dataset<String> words = lines
.as(Encoders.STRING())
.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

// Generate running word count
Dataset<Row> wordCounts = words.groupBy("value").count();
{% endhighlight %}

This `lines` DataFrame represents an unbounded table containing the streaming text data. This table contains one column of strings named "value", and each line in the streaming text data becomes a row in the table. Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it. Next, we have converted the DataFrame to a Dataset of String using `.as(Encoders.STRING())`, so that we can apply the `flatMap` operation to split each line into multiple words. The resultant `words` Dataset contains all the words. Finally, we have defined the `wordCounts` DataFrame by grouping by the unique values in the Dataset and counting them. Note that this is a streaming DataFrame which represents the running word counts of the stream.

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}

# Create DataFrame representing the stream of input lines from connection to localhost:9999

lines <- read.stream("socket", host = "localhost", port = 9999)

# Split the lines into words

words <- selectExpr(lines, "explode(split(value, ' ')) as word")

# Generate running word count

wordCounts <- count(group_by(words, "word"))
{% endhighlight %}

This `lines` SparkDataFrame represents an unbounded table containing the streaming text data. This table contains one column of strings named "value", and each line in the streaming text data becomes a row in the table. Note, that this is not currently receiving any data as we are just setting up the transformation, and have not yet started it. Next, we have a SQL expression with two SQL functions - split and explode, to split each line into multiple rows with a word each. In addition, we name the new column as "word". Finally, we have defined the `wordCounts` SparkDataFrame by grouping by the unique values in the SparkDataFrame and counting them. Note that this is a streaming SparkDataFrame which represents the running word counts of the stream.

</div>

</div>

## Starting the query

We have now set up the query on the streaming data. All that is left is to actually start receiving data and computing the counts. To do this, we set it up to print the complete set of counts (specified by `outputMode("complete")`) to the console every time they are updated. And then start the streaming computation using `start()`.

<div class="codetabs">

<div data-lang="python"  markdown="1">

{% highlight python %}

# Start running the query that prints the running counts to the console

query = wordCounts \
 .writeStream \
 .outputMode("complete") \
 .format("console") \
 .start()

query.awaitTermination()
{% endhighlight %}

</div>

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// Start running the query that prints the running counts to the console
val query = wordCounts.writeStream
.outputMode("complete")
.format("console")
.start()

query.awaitTermination()
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// Start running the query that prints the running counts to the console
StreamingQuery query = wordCounts.writeStream()
.outputMode("complete")
.format("console")
.start();

query.awaitTermination();
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}

# Start running the query that prints the running counts to the console

query <- write.stream(wordCounts, "console", outputMode = "complete")

awaitTermination(query)
{% endhighlight %}

</div>

</div>

After this code is executed, the streaming computation will have started in the background. The `query` object is a handle to that active streaming query, and we have decided to wait for the termination of the query using `awaitTermination()` to prevent the process from exiting while the query is active.

To actually execute this example code, you can either compile the code in your own
[Spark application](quick-start.html#self-contained-applications), or simply
[run the example](index.html#running-the-examples-and-shell) once you have downloaded Spark. We are showing the latter. You will first need to run Netcat (a small utility found in most Unix-like systems) as a data server by using

    $ nc -lk 9999

Then, in a different terminal, you can start the example by using

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight bash %}
$ ./bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py localhost 9999
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight bash %}
$ ./bin/run-example org.apache.spark.examples.sql.streaming.StructuredNetworkWordCount localhost 9999
{% endhighlight %}
</div>

<div data-lang="java"  markdown="1">
{% highlight bash %}
$ ./bin/run-example org.apache.spark.examples.sql.streaming.JavaStructuredNetworkWordCount localhost 9999
{% endhighlight %}
</div>

<div data-lang="r"  markdown="1">
{% highlight bash %}
$ ./bin/spark-submit examples/src/main/r/streaming/structured_network_wordcount.R localhost 9999
{% endhighlight %}
</div>

</div>

Then, any lines typed in the terminal running the netcat server will be counted and printed on screen every second. It will look something like the following.

<table width="100%">
    <td>
{% highlight bash %}
# TERMINAL 1:
# Running Netcat

$ nc -lk 9999
apache spark
apache hadoop

...
{% endhighlight %}

</td>
<td width="2%"></td>
<td>

<div class="codetabs">

<div data-lang="python" markdown="1">
{% highlight bash %}
# TERMINAL 2: RUNNING structured_network_wordcount.py

$ ./bin/spark-submit examples/src/main/python/sql/streaming/structured_network_wordcount.py localhost 9999

---

## Batch: 0

+------+-----+
| value|count|
+------+-----+
|apache| 1|
| spark| 1|
+------+-----+

---

## Batch: 1

+------+-----+
| value|count|
+------+-----+
|apache| 2|
| spark| 1|
|hadoop| 1|
+------+-----+
...
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight bash %}
# TERMINAL 2: RUNNING StructuredNetworkWordCount

$ ./bin/run-example org.apache.spark.examples.sql.streaming.StructuredNetworkWordCount localhost 9999

---

## Batch: 0

+------+-----+
| value|count|
+------+-----+
|apache| 1|
| spark| 1|
+------+-----+

---

## Batch: 1

+------+-----+
| value|count|
+------+-----+
|apache| 2|
| spark| 1|
|hadoop| 1|
+------+-----+
...
{% endhighlight %}

</div>

<div data-lang="java" markdown="1">
{% highlight bash %}
# TERMINAL 2: RUNNING JavaStructuredNetworkWordCount

$ ./bin/run-example org.apache.spark.examples.sql.streaming.JavaStructuredNetworkWordCount localhost 9999

---

## Batch: 0

+------+-----+
| value|count|
+------+-----+
|apache| 1|
| spark| 1|
+------+-----+

---

## Batch: 1

+------+-----+
| value|count|
+------+-----+
|apache| 2|
| spark| 1|
|hadoop| 1|
+------+-----+
...
{% endhighlight %}

</div>

<div data-lang="r" markdown="1">
{% highlight bash %}
# TERMINAL 2: RUNNING structured_network_wordcount.R

$ ./bin/spark-submit examples/src/main/r/streaming/structured_network_wordcount.R localhost 9999

---

## Batch: 0

+------+-----+
| value|count|
+------+-----+
|apache| 1|
| spark| 1|
+------+-----+

---

## Batch: 1

+------+-----+
| value|count|
+------+-----+
|apache| 2|
| spark| 1|
|hadoop| 1|
+------+-----+
...
{% endhighlight %}

</div>
</div>
    </td>
</table>
