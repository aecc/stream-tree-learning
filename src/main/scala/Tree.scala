import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream

/**
 * @author aecc
 * Creation of the decision tree
 */
object Tree {
	
	def makeDecisionTree(data_set: RDD[(Int, (Array[Int], Int, Int, Int))], attributes: Array[String], classes: Array[String] ) {
		val attribute_values = data_set.context.broadcast(new AttributeValues(attributes))
		// TODO: remove
		data_set.foreach {
			case (_,(_,_,_,_)) => {
				attribute_values.value.getValuesNames("number_words_title").foreach(println)
			}
		}
	}
	
	def bestSplit(data_set: RDD[(Int, (Array[Int], Int, Int, Int))], attributes: Array[String], classes: Array[String] ) {
		
	}

}