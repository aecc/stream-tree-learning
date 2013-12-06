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
	
	def makeDecisionTree(	data_set: RDD[(Int, (Array[Int], Int, Int, Int))], 
							attributes: Array[String], 
							classes: Array[String]) 
							: RDD[(Int, (Array[Int], Int, Int, Int))] = {
		
		val attribute_values = data_set.context.broadcast(new AttributeValues(attributes))
		// TODO: remove
		data_set.filter {
			case (_,(Array(number_words, attention, engagement, rating),_,_,_)) => {
				val title_longer_than_k = attribute_values.value.getValues("number_words_title")(0)
				title_longer_than_k(16)
			}
		}
	}
	
	def bestSplit(data_set: RDD[(Int, (Array[Int], Int, Int, Int))], attributes: Array[String], classes: Array[String] ) {
		
	}

}