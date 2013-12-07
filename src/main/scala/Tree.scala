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
	
	/*
	 * Main function for the creation of the tree 
	 * Returns an RDD of Chains
	 */
	def makeDecisionTree(	dataRDD: RDD[(Int, (Array[Int], Int, Int, Int))], 
							attributes: Array[String], 
							classes: Array[String]) 
							: RDD[Chain] = {
		
		// Max length of the tree
		val max_depth = attributes.length
		
		val attribute_values = dataRDD.context.broadcast(new AttributeValues(attributes))
		
		// First split to get first best feature
		val (feature,values) = bestSplit(dataRDD,attribute_values.value.attributes.toArray,classes)
		
		// Start the tree building. A chain on each value
		var chainSet = dataRDD.context.parallelize(values).map(value => new Chain(feature,value))
		
		var i = 1
		while (i <= max_depth) {
			
			chainSet.filter(_.chain.length == i).foreach(chain => {
				
				val attrs = dataRDD.context.broadcast(chain.getAttributes)
				val possible_attributes = chain.getNextPossibleAttributes(attribute_values.value.attributes.toArray) 
				
				// We filter data according to the attributes in the chain
				val sampleRDD = dataRDD.filter(entry => {attribute_values.value.checkEntryAttributesValues(entry, attrs)}) 
				
				// Find the best split among the attributes remaining
				val (feature,values) = bestSplit(sampleRDD,possible_attributes,classes)
				
				if (feature != null) {
					//val attribute
					chainSet = chainSet ++ dataRDD.context.parallelize(values).map({
							value => { 
								val new_chain = new Chain(feature,value)
								new_chain.chain = chain.chain ++ new_chain.chain
								new_chain
							}
					})
				}
			
			})
			
			i = i+1
		}		
		
		// TODO: remove
		/*
		dataRDD.filter {
			case (image_id,(Array(number_words, attention, engagement, rating),_,_,_)) => {
				val title_longer_than_k = attribute_values.value.getValues("number_words_title")(0)
				title_longer_than_k(number_words)
			}
		}
		* */
		chainSet
		
	}
	
	/*
	 * TODO
	 */
	def bestSplit(	data_set: RDD[(Int, (Array[Int], Int, Int, Int))], 
					attributes: Array[(String, Array[Int => Boolean])], 
					classes: Array[String]) 
					: (String, Array[Int => Boolean]) = {
		
		attributes(0)		
	}

}