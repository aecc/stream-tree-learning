import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast.Broadcast


object BestSplit{
	
	/*
	 * Gets the best split of to get the next better attribute
	 */
	def bestSplit(	dataRDD: RDD[(Int, (Array[Int], Int, Int, Int))],
					entropy_before: Double,
					attributes: Array[(String, Array[Int => Boolean])], 
					att_values: Broadcast[AttributeValues], 
					classes: Array[String]) 
					: ((String, Array[Int => Boolean]),Double) = {
		
		if (attributes.isEmpty) return ((null,null),0.0)
		
		val attribute_values = att_values.value
		val features_array = attribute_values.attributes.toArray
		val numbers = (0 until features_array.size).toArray 
		
		// Get the entropy for each of the attributes
		val entropies = numbers.map(number => {
			
			val current_feature = features_array(number)
			
			// Check if it's in the list of attributes that we want to check 
			var filter = false
			for (att <- attributes) {	
				if (att.equals(current_feature)) {
					filter = true
				}
			}
			
			if (filter) {
				val featureRDD = dataRDD.map {
					case (_,(features,_,_,class_value)) => {
						(features(number),class_value)
					}
				}
				// Calculate the entropy for this feature given the data set
				Entropy.calculateEntropy(featureRDD, classes)
			} else {
				0.0
			}
			
		})	
		
		// Choose the entropy that maximizes the infomation gain
		val bestIG = entropies.map(entropy => {
			entropy_before - entropy
		}).max
		
		val best_feature_index = entropies.indexOf(bestIG)
		val best_feature = features_array(best_feature_index)
		((best_feature._1,best_feature._2),entropies(best_feature_index))	
	}

}
