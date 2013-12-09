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
		
		if (attributes.length==1) return ((null,null),0.0)
		
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
				
				// Check entropy for each value
				var min_entropy = 1.0
				for (value <- current_feature._2) {
				
					val featureAndValueRDD = featureRDD.filter(entry => {
						val attr_values = featureRDD.context.broadcast(Array((current_feature._1,value)))
						attribute_values.checkParamValues(entry._1, attr_values)
					})
					
					// Calculate the entropy for this feature given the data set
					val entropy = Entropy.calculateEntropy(featureAndValueRDD, classes)
					if (entropy<min_entropy)
						min_entropy = entropy
				}
				
				// Return the minimum entropy
				min_entropy
				
			} else {
				1.0
			}
			
		})	
		
		// Choose the entropy that maximizes the infomation gain
		val bestIGs = entropies.map(entropy => {
			(entropy,entropy_before - entropy)
		})
		
		val bestIG = bestIGs.maxBy(_._2)
		val best_feature_index = entropies.indexOf(bestIG._1)
		val best_feature = features_array(best_feature_index)
		((best_feature._1,best_feature._2),entropies(best_feature_index))	
	}

}
