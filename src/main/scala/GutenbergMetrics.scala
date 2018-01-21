import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.log4j.{Level,Logger}
import metricUtils.ClusterMetricSetup._

object GutenbergMetrics {

 
  def main(args: Array[String]): Unit = {
    
    Logger.getRootLogger.setLevel(Level.ERROR) // minimum message level so not verbose
    val conf = clusterMetricsConfig().setAppName("Gutenberg word counting with metrics")
    val sc =  new SparkContext(conf)

    val log = Logger.getLogger(getClass.getName)  
    val myListeners = new metricUtils.ExtraSparkListener(sc.applicationId, sc.sparkUser, log)
    sc.addSparkListener(myListeners)

    val gutenbergVanillaRDD = sc.textFile("hdfs://moonshot-ha-nameservice/data/gutenberg")
    val searchString = { if (args.length > 0) args(0) else "viva" }
    
    val vanillaPartitions = gutenbergVanillaRDD.partitions.size
    val numPartsRequested = if (args.length > 1) args(1).toInt else 15
    val gutenbergLinesRDD = {
           if      (vanillaPartitions < numPartsRequested) gutenbergVanillaRDD.repartition(numPartsRequested)
           else if (vanillaPartitions > numPartsRequested) gutenbergVanillaRDD.coalesce(numPartsRequested)
           else                                            gutenbergVanillaRDD
    }
    gutenbergLinesRDD.setName("gutenbergLinesRDD")

    val appStart = System.currentTimeMillis // timing measures start after partitioning

    println ("++> " + gutenbergLinesRDD.count() + 
             " lines ingested from Gutenberg into " + gutenbergLinesRDD.partitions.size + " partitions")

    val gutenbergWordsRDD = gutenbergLinesRDD.flatMap(line => line.split("[ .;:()?\"]+")).setName("gutenbergWordsRDD")
    println("++> " + gutenbergWordsRDD.count() + " words in Gutenberg")

    val wordMapperResultsRDD = gutenbergWordsRDD.map(word => (word.toLowerCase(),1)).setName("wordMapperResultsRDD")
    val wordReducerResultsRDD = wordMapperResultsRDD.reduceByKey(_ + _).setName("wordReducerResultsRDD")
    println("++> " + wordReducerResultsRDD.count() + " unique words in Gutenberg")


    val matchesRDD = wordReducerResultsRDD.filter(wordCountPair => wordCountPair._1.equals(searchString.toLowerCase()))
    val occurrences = matchesRDD.map(t => t._2).sum().toInt
    println("++> The word \"" + searchString + "\" occurs " + occurrences + " times in Gutenberg")

    println("++> Top Ten are:")
    wordReducerResultsRDD.filter(wc => wc._1.size > 0).sortBy(_._2,false).take(10).foreach(x => { val w = x._1
                                                                                                  val c = x._2
                                                                                                  println(f"++> $w%30s : $c%10d")})

    val appEnd = System.currentTimeMillis
    println ("\n++> Elapsed time for " + sc.applicationId + " was " + (appEnd - appStart)/1000 + " seconds (after repartitioning)")

    clusterMetricPostProcessing(sc, true)
    
  }
}
