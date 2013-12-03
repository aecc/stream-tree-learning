import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream

/*
* Entropy calculation module
*/
object Entropy {
	
	def calculateEntropy(classes: ClassesSet, data: RDD[String]) {
		val total_data = data.count
		for (i <- 0 until classes.size) {
			// We filter the RDD to get only the classes that we need
			data.filter(f)
		}
	}
	
}