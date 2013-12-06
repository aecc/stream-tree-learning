import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j._

object StreamTreeLearning {
  
	def main(args: Array[String]) {
	  
		// Parameter to decide if post is reposted or no more then k times
		val k_parameter = 7
		if (args.length < 3) {
			System.err.println("Usage: StreamTreeLearning <master> <ip-stream> <port>")
			System.exit(1)
		}

		val logger = Logger.getLogger(getClass().getName());
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
		
		// External data structure to save reposts per image_id
		var reposts = ssc.sparkContext.parallelize(Array((0,0)))
		
		// TODO: Change to DStream
		// Transform the RDDs coming from the stream using the following process
		val filtered = reddits_stream.transform(rdd => {
			val filteredRDD = FilterProcess.filter(rdd,k_param)
			val mixedRDD = FilterProcess.mixReposts(filteredRDD, reposts, k_param)
			// TODO: EXTREMELY UNEFFICIENT, MAYBE A BOUNDED SET reposts
			//reposts = FilterProcess.getRepostsByKey(filteredRDD, reposts)
			reposts.persist
			val treeRDD = Tree.makeDecisionTree(mixedRDD, Array("number_words_title"), Array("Yes",""))
			treeRDD
		})
		
		// TODO: remove
		
		val reduced_dstream = filtered.filter({ 
			case (id,(_,_,_,_)) => { 
				if (id==710455) true else false 
			} 
		}).print
		
		
		//filtered.print
		
		// Start the computation
		ssc.start()

	}

}
