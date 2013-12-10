import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import org.apache.spark.broadcast.Broadcast
import org.apache.log4j.Logger


object BestSplit{
	
	val logger = Logger.getLogger(getClass().getName());
	
	/*
	 * Gets the best split of to get the next better attribute
	 * Returns best feature with values, the array of entropies and the index of the entropy
	 */
	def bestSplit(	dataRDD: RDD[(Int, (Array[Int], Int, Int, Int))],
					entropy_before: Double,
					attributes: Array[(String, Array[Int => Boolean])], 
					att_values: Broadcast[AttributeValues], 
					classes: Array[String]) 
					: ((String, Array[Int => Boolean]),Array[Double]) = {
		
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
				
				var entropies_per_value = new Array[Double](0)
				// Check entropy for each value
				var sum = 0.0
				
				for (value <- current_feature._2) {
				
					val attr_values = featureRDD.context.broadcast(Array((current_feature._1,value)))
					val featureAndValueRDD = featureRDD.filter(entry => {
						attribute_values.checkParamValues(entry._1, attr_values)
					})
					
					// Calculate the entropy for this feature given the data set
					val entropy = Entropy.calculateEntropy(featureAndValueRDD, classes)
					entropies_per_value = entropies_per_value :+ entropy
					
					sum += entropy
					
				}
				
				// Return the entropies
				(sum,entropies_per_value)
				
			} else {
				(Double.MaxValue,Array[Double]())
			}
			
		})	
				
		// Choose the entropy that maximizes the infomation gain
		val bestIGs = entropies.map(entropy => {
			(entropy,entropy_before - entropy._1)
		})
		
		val bestIG = bestIGs.maxBy(_._2)
		val final_entropies = bestIG._1._2
		
		val best_feature_index = entropies.indexOf(bestIG._1)
		val best_feature = features_array(best_feature_index)
		((best_feature._1,best_feature._2),final_entropies)	
	}

}
