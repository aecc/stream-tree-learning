import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j._

object StreamTreeLearning {
  
	def main(args: Array[String]) {
	  
		if (args.length < 3) {
			System.err.println("Usage: StreamTreeLearning <master> <ip-stream> <port>")
			System.exit(1)
		}

		val logger = Logger.getLogger(getClass().getName());
		logger.info("Logger is working!")
		val ssc = new StreamingContext(args(0), "StreamTreeLearning", Seconds(2),
		System.getenv("SPARK_HOME"), List("target/scala-2.9.3/stream-tree-learning_2.9.3-1.0.jar"))

		val reddits = ssc.socketTextStream(args(1), args(2).toInt)
		val filtered = FilterProcess.filter(reddits)
		
		// Start the computation
		ssc.start()

	}

}
