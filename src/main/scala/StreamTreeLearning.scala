import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j._

object StreamTreeLearning {
  
	val logger = Logger.getLogger(getClass().getName());
	logger.setLevel(Level.INFO)
	
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
										Seconds(60), 
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
			logger.info("Number of entries in this RDD: " + rdd.count)
			if (rdd.count != 0) {
				logger.info("Starting filtering data... [1/4]")
				val filteredRDD = FilterProcess.filter(rdd,k_param)
				filteredRDD.persist
				logger.info("Finished filtering data [1/4]")
				logger.info("Starting mixing with old data... [2/4]")
				val mixedRDD = FilterProcess.mixReposts(filteredRDD, reposts, k_param)
				logger.info("Finished mixing with old data [2/4]")
				// TODO: EXTREMELY UNEFFICIENT, MAYBE A BOUNDED SET reposts
				//reposts = FilterProcess.getRepostsByKey(filteredRDD, reposts)
				reposts.persist
				logger.info("Starting decision tree making... [3/4]")
				val treeRDD = Tree.makeDecisionTree(mixedRDD, attributes, classes)
				treeRDD.persist
				val chainSet = treeRDD.context.broadcast(treeRDD)
				logger.info("Finished decision tree making [3/4]")
				logger.info("Starting the evaluation part... [4/4]")
				// TODO: right now doing it with same data, should be a different one
				val evaluationRDD = filteredRDD.map(entry => {
					(entry._2._4, Evaluate.predictEntry(entry, chainSet.value, classes))
				})
				val error = evaluationRDD.map(tuple => {
					if (tuple._1 == tuple._2) 0
					else 1
				}).reduce(_+_).toDouble / filteredRDD.count
				
				logger.info("Finished the evaluation part [4/4]")
				logger.info("The error of the prediction is: " + error)
		
				filteredRDD.unpersist(false)
				// TODO remove!
				//ssc.stop
				rdd.context.parallelize(Array(error))
			} else {
				logger.info("No data. Nothing to do")
				rdd.context.parallelize(Array(1.0))
			}
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
