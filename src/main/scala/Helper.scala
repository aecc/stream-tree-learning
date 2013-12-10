import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

/**
 * @author aecc
 * Helper to add random functions with usage for testing, etc
 */
object Helper {

	/*
	 * Filter RDD by Class number
	 */
	def filterByClass(data: RDD[(Int, (Array[Int], Int, Int, Int))], class_to_use: Int ) : RDD[(Int, (Array[Int], Int, Int, Int))] = {
		val filtered = data.filter { 
			case (_,(_,_,_,class_value)) => {
				if (class_value==class_to_use) 
					true
				else
					false
			}
		}
		filtered
	}
	
	/*
	 * Filter one feature RDD by Class number
	 */
	def filterFeatureByClass(data: RDD[(Int, Int)], class_to_use: Int ) : RDD[(Int, Int)] = {
		val filtered = data.filter { 
			case (_,class_value) => {
				if (class_value==class_to_use) 
					true
				else
					false
			}
		}
		filtered
	}
	
	
	/*
	 * Filter by maximum unix time of the post 
	 */
	def filterByTime(data: RDD[(Int, (Array[Int], Int, Int, Int))], max_time: Int ) : RDD[(Int, (Array[Int], Int, Int, Int))] = {
		val filtered = data.filter { 
			case (_,(_,unixtime,_,_)) => {
				if (unixtime<max_time) 
					true
				else
					false
			}
		}
		filtered
	}
	
	/*
	 * Get the maximum number of reposts in the dataset
	 */
	def getMaxReposts(data: RDD[(Int, (Array[Int], Int, Int, Int))]) : Int = {

		val f = data.map {
			case (_,(_,_,times,_)) => {
				(0,times)
			}
		}.reduceByKey((l1,l2) => {
				if (l1>l2) 
					l1
				else 
					l2
		})
		f.first._2
	}
	
	def loadTestData() {
		/*
		import org.apache.spark._
		val sc = new SparkContext("local", "shell")
		val small = sc.textFile("/home/aecc/git/stream-tree-learning/files/redditSubmissions-small.csv", 2).cache()
		val k_parameter = sc.broadcast(2)
		val sfiltered = FilterProcess.filter(small,k_parameter)
		val attributes = Array("number_words_title","attention", "rating", "engagement")
		val classes = Array("Reposted_Less_Than_K","Reposted_More_Equal_Than_K")
		val attribute_values = sc.broadcast(new AttributeValues(attributes))
		val ((feature,values),entropies) = BestSplit.bestSplit(sfiltered, 1.0, attribute_values.value.attributes.toArray, attribute_values, classes)
		*/
	}
}