import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j._

object StreamTreeLearning {
  
	val logger = Logger.getLogger(getClass().getName());
	
	def main(args: Array[String]) {
	  
		// Parameter to decide if post is reposted or no more then k times
		val k_parameter = 2
		if (args.length < 3) {
			System.err.println("Usage: StreamTreeLearning <master> <ip-stream> <port>")
			System.exit(1)
		}

		logger.info("Logger is working!")
		val ssc = new StreamingContext(	args(0), 
										"StreamTreeLearning", 
										Seconds(2), 
										System.getenv("SPARK_HOME"), 
										List("target/scala-2.9.3/stream-tree-learning_2.9.3-1.0.jar"),
										Map())

		val reddits_stream = ssc.socketTextStream(args(1), args(2).toInt)
		
		// We broadcast the k parameter, because it's a read-only variable
		val k_param = ssc.sparkContext.broadcast(k_parameter)
		
		// Attributes and classes
		val attributes = Array("number_words_title","attention", "rating", "engagement")
		val classes = Array("Reposted_Less_Than_K","Reposted_More_Equal_Than_K")
		
		// External data structure to save reposts per image_id
		var reposts = ssc.sparkContext.parallelize(Array((0,0)))
		
		logger.info("Starting the stream process...")
		
		// TODO: Change to DStream
		// Transform the RDDs coming from the stream using the following process
		val filtered = reddits_stream.transform(rdd => {
			val filteredRDD = FilterProcess.filter(rdd,k_param)
			logger.info("Finished filtering data [1/3]")
			val mixedRDD = FilterProcess.mixReposts(filteredRDD, reposts, k_param)
			logger.info("Finished mixing with old data [2/3]")
			// TODO: EXTREMELY UNEFFICIENT, MAYBE A BOUNDED SET reposts
			//reposts = FilterProcess.getRepostsByKey(filteredRDD, reposts)
			reposts.persist
			val treeRDD = Tree.makeDecisionTree(mixedRDD, attributes, classes)
			logger.info("Finished decision tree making [3/3]")
			Evaluate.predictEntry(filteredRDD.first, treeRDD, classes)
			// TODO remove!
			ssc.stop
			treeRDD
		})
		
		// TODO: remove
		/*
		val reduced_dstream = filtered.filter({ 
			case (id,(_,_,_,_)) => { 
				if (id==710455) true else false 
			} 
		}).print
		*/
		
		filtered.print
		
		// Start the computation
		ssc.start()

	}

}
