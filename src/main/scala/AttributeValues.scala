import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream
import scala.collection.mutable.HashMap

/**
 * @author aecc
 * Class to define all the attribute values
 * This needs to be broadcasted at the beginning!!
 */
class AttributeValues(attrs: Array[String]) extends Serializable {
	
	val attributes = new HashMap[String,Array[Int => Boolean]]()
	val attributes_name = new HashMap[String,Array[String]]()
	
	// Functions (attribute values)
	val title_shorter_than_k : Int => Boolean = _ < 4
	val title_longer_than_k : Int => Boolean = _ > 16
	
	attrs.foreach(attr => {
		attr match {
			// Bind attributes to attributes values
			case "number_words_title" => 
				attributes(attr) = Array(	title_longer_than_k,
											title_shorter_than_k)
				attributes_name(attr) = Array(	"title_longer_than_k",
												"title_shorter_than_k")
		}
	
		 
	})
	
	def getValues(attr: String) : Array[Int => Boolean] = {
		attributes(attr)
	}
	
	def getValuesNames(attr: String) : Array[String] = {
		attributes_name(attr)
	}
	
	
}