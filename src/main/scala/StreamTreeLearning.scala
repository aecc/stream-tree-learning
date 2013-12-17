import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j._

object StreamTreeLearning {
  
	var sc : SparkContext = _
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
		sc = ssc.sparkContext

		val reddits_stream = ssc.socketTextStream(args(1), args(2).toInt)
		
		// We broadcast the k parameter, because it's a read-only variable
		val k_param = ssc.sparkContext.broadcast(k_parameter)
		
		// Attributes and classes
		val attributes = Array("number_words_title","attention", "rating", "engagement")
		val classes = Array("Reposted_Less_Than_K","Reposted_More_Equal_Than_K")
		
		// Best tree built so far
		var bestRDD : RDD[Chain] = null
		var bestError : Double = 1.0
		
		val attribute_values = StreamTreeLearning.sc.broadcast(new AttributeValues(attributes))
		
		// External data structure to save reposts per image_id
		//var reposts = ssc.sparkContext.parallelize(Array((0,0)))
		
		logger.info("Starting the stream process...")
		
		// TODO: Change to DStream
		// Transform the RDDs coming from the stream using the following process
		val filtered = reddits_stream.transform(rdd => {
			logger.info("Number of entries in this RDD: " + rdd.count)
			if (rdd.count != 0) {
								
				//val rddModel = rdd.filter(f)
				
				logger.info("Starting filtering data... [1/4]")
				val filteredRDD = FilterProcess.filter(rdd,k_param).persist
				
				// We get 80% of the data for creating the model and 20% to evaluate
				val lines = filteredRDD.collect
				val split_index = (filteredRDD.count * 80)/100
				val (lines_model,lines_test) = lines.splitAt(split_index.toInt)
				
				val modelRDD = sc.parallelize(lines_model)
				println("model:" + modelRDD.count)
				println("--")
				val testRDD = sc.parallelize(lines_test)
				println("test:" + testRDD.count)
				
				logger.info("Finished filtering data [1/4]")
				logger.info("Starting mixing with old data... [2/4]")
				//val mixedRDD = FilterProcess.mixReposts(filteredRDD, reposts, k_param)
				//mixedRDD.persist
				logger.info("Finished mixing with old data [2/4]")
				// TODO: EXTREMELY UNEFFICIENT, MAYBE A BOUNDED SET reposts
				//reposts = FilterProcess.getRepostsByKey(filteredRDD, reposts)
				//reposts.persist
				logger.info("Starting decision tree making... [3/4]")
				val treeRDD = Tree.makeDecisionTree(modelRDD, attribute_values.value, classes)
				treeRDD.persist
		
				val chainSet = sc.broadcast(treeRDD)
				logger.info("Finished decision tree making [3/4]")
				logger.info("Starting the evaluation part... [4/4]")
				
				// Evaluate the accuracy of the tree using the test Data
				val evaluationRDD = testRDD.map(entry => {
					(entry._2._4, Evaluate.predictEntry(entry, chainSet.value, classes, attribute_values.value))
				})
				
				val error = evaluationRDD.map(tuple => {
					if (tuple._1 == tuple._2) 0
					else 1
				}).reduce(_+_).toDouble / testRDD.count
				
				logger.info("Finished the evaluation part [4/4]")
				logger.info("The error of the prediction is: " + error)
				
				// We choose the best tree
				if (bestRDD == null) {
					bestRDD = treeRDD
				} else {
					val bestChainSet = sc.broadcast(bestRDD)
					val bestError = filteredRDD.map(entry => {
									(entry._2._4, Evaluate.predictEntry(entry, bestChainSet.value, classes, attribute_values.value))
									}).map(tuple => {
									if (tuple._1 == tuple._2) 0
									else 1
									}).reduce(_+_).toDouble / filteredRDD.count
					logger.info("The error of the prediction with the best tree is: " + bestError)
					if (error < bestError) {
						bestRDD = treeRDD
						logger.info("A new tree has been set as the best one")
					}
				}

				rdd.context.parallelize(Array(error))
			} else {
				logger.info("No data. Nothing to do")
				rdd.context.parallelize(Array(1.0))
			}
		})

		filtered.print
		
		// Start the computation
		ssc.start()

	}

}
