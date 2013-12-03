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
	
	/*
	 * Calculate the entropy value
	 * RDD is in format: (Image_id, (Array(features), unixtime, number of times reposted, class value))
	 * At first: Image_id, (Array(number of words, attention, engagement, rating), unixtime, number of times reposted, class value)
	 * (10003,(Array(8,127,11,10),1321941344,5,1))
	 */
	def calculateEntropy(classes: ClassesSet, data: RDD[(Int, Any, Int, Int, Int)]) {
		val total_data = data.count
		for (i <- 0 until classes.size) {
			// We filter the RDD to get only the classes that we need
			//ata.filter(f)
			
		}
	}
	
}