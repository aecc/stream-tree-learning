import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream
import scala.collection.mutable.LinkedHashMap
import org.apache.spark.broadcast.Broadcast

/**
 * @author aecc
 * Class to define all the attribute values
 * This needs to be broadcasted at the beginning!!
 */
class AttributeValues(attrs: Array[String]) extends Serializable {
	
	val attributes = new LinkedHashMap[String,Array[Int => Boolean]]()
	val attributes_name = new LinkedHashMap[String,Array[String]]()
	
	// Functions (attribute values)
	val title_in_range : Int => Boolean = (k => {k >= 4 && k <= 16})
	val title_outside_range : Int => Boolean = (k => {k < 4 || k > 16})
	
	val rating_negative: Int => Boolean = (k => {k < 0})
	val rating_first_range: Int => Boolean = (k => {k >= 0 && k<100})
	val rating_second_range: Int => Boolean = (k => {k >= 100})
	
	val attention_first_range: Int => Boolean = (k => {k <= 500})
	val attention_second_range: Int => Boolean = (k => {k > 500 && k<1000})
	val attention_third_range: Int => Boolean = (k => {k >= 1000})
	
	val engagement_zero: Int => Boolean = (k => {k == 0})
	val engagement_first_range: Int => Boolean = (k => {k > 0 && k<=50})
	val engagement_second_range: Int => Boolean = (k => {k > 50 && k<=100})
	val engagement_third_range: Int => Boolean = (k => {k > 100})
	
	attrs.foreach(attr => {
		attr match {
			// Bind attributes to attributes values
			case "number_words_title" => 
				attributes(attr) = Array(title_in_range, title_outside_range)
				attributes_name(attr) = Array(	"title_in_range", "title_outside_range")
			
			case "rating" =>
				attributes(attr) = Array(rating_negative, rating_first_range, rating_second_range)
				attributes_name(attr) = Array("rating_negative", "rating_first_range", "rating_second_range")
			
			case "attention" =>
				attributes(attr) = Array(attention_first_range, attention_second_range, attention_third_range)
				attributes_name(attr) = Array("attention_first_range", "attention_second_range", "attention_third_range")
			
			case "engagement" =>
				attributes(attr) = Array(engagement_zero, engagement_first_range, engagement_second_range, engagement_third_range)
				attributes_name(attr) = Array("engagement_zero", "engagement_first_range", "engagement_second_range", "engagement_third_range")
			
			case _ =>
				attributes(attr) = Array()
				attributes_name(attr) = Array()
		}
	
		 
	})
	
	/*
	 * Get the predicates of the given attribute
	 */
	def getValues(attr: String) : Array[Int => Boolean] = {
		attributes(attr)
	}
	
	/*
	 * Get the values names of the given attribute
	 */
	def getValuesNames(attr: String) : Array[String] = {
		attributes_name(attr)
	}
	
	
	/*
	 * Check if the entry given matches with all the attribute values in the array
	 */
	def checkEntryAttributesValues(	entry: (Int, (Array[Int], Int, Int, Int)), 
									attrs_values: Broadcast[Array[(String,Int => Boolean)]])
									: Boolean = {
		
		val values_array = entry._2._1
		val attributes_array = attributes.toArray
		var i = 0
		var filter = true
		
		while (i<values_array.length) {
			filter = filter && checkParamAttributesValues(attributes_array(i)._1, values_array(i), attrs_values)
			i = i+1
		}
		filter
		
	}	
	
	/*
	 * Check if the param given matches with all the attribute values in the array
	 */
	def checkParamAttributesValues(	feature: String,
									param: Int, 
									attrs_values: Broadcast[Array[(String,Int => Boolean)]])
									: Boolean = {
		
		var filter = true
		attrs_values.value.foreach(attr => {
			for (att <- getValuesNames(feature)) {				
				if (attr._1.equals(att)) {
					val func = attr._2
					filter = filter && func(param)
				}
			}
		})	
		filter
		
	}
	
	
	/*
	 * Check if the param given matches with all the attribute values in the array
	 */
	def checkParamValues(	param: Int, 
							attrs_values: Broadcast[Array[(String,Int => Boolean)]])
							: Boolean = {
		
		var filter = true
		attrs_values.value.foreach(attr => {		
			val func = attr._2
			filter = filter && func(param)
		})	
		filter
		
	}
}
