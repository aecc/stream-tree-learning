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
	def calculateEntropy(	data: RDD[(Int, Int)], 
							classes: Array[String]) 
							: Double = {
		
		val total_data_count = data.count
		if (total_data_count==0) return 1.0
		
		var sum = 0.0
		for (i <- 0 until classes.size) {
			
			// We filter the RDD to get only the classes that we need
			val filtered = Helper.filterFeatureByClass(data, i)
			
			val number_matches = filtered.count
			val p_class_given_total = number_matches.toFloat/total_data_count
			if (p_class_given_total!=0.0)
				sum += p_class_given_total * ( Math.log(p_class_given_total) / Math.log(2) )	
			
		}
		-sum		
	}
	
}