import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream
import org.apache.spark.Accumulable
import scala.collection.mutable.Queue
import org.apache.log4j.Logger

/**
 * @author aecc
 * Creation of the decision tree
 */
object Tree {
	
	val logger = Logger.getLogger(getClass().getName());
	
	/*
	 * Main function for the creation of the tree 
	 * Returns an RDD of Chains
	 */
	def makeDecisionTree(	dataRDD: RDD[(Int, (Array[Int], Int, Int, Int))], 
							attributes: Array[String], 
							classes: Array[String]) 
							: RDD[Chain] = {
		
		logger.info("Creating the tree...")
		
		// Max length of the tree
		val max_depth = attributes.length
		
		val attribute_values = dataRDD.context.broadcast(new AttributeValues(attributes))
		
		// First split to get first best feature
		val ((feature,values),entropy) = BestSplit.bestSplit(dataRDD, 0.0, attribute_values.value.attributes.toArray, attribute_values, classes)
		logger.info("First Best split is " + feature + " with entropy " + entropy)
		
		// Start the tree building. A chain on each value
		var chainSet = dataRDD.context.parallelize(values).map(value => new Chain(feature,value))
		
		// Accumulator for chains
		val chains_accum = dataRDD.context.accumulableCollection[Queue[Chain],Chain](Queue[Chain]())	
		
		var i = 1
		while (i <= max_depth) {

			logger.info("Creating branches at depth "+i+"...")
			// TODO test, filter should be redundant now
			chainSet.filter(_.chain.length == i).foreach(chain => {
				/*
				val attrs = dataRDD.context.broadcast(chain.getAttributes)
				val possible_attributes = chain.getNextPossibleAttributes(attribute_values.value.attributes.toArray) 
				
				// We filter data according to the attributes in the chain
				val sampleRDD = dataRDD.filter(entry => {attribute_values.value.checkEntryAttributesValues(entry, attrs)}) 
				sampleRDD.persist
				
				// Find the best split among the attributes remaining
				val ((feature,values),entropy) = BestSplit.bestSplit(sampleRDD, chain.entropy, possible_attributes, attribute_values, classes)
				
				logger.info("Best split is " + feature)
				
				// If we still have attributes to create
				if (feature != null) {
					val new_chains = values.map({
							value => { 
								val new_chain = new Chain(feature,value)
								new_chain.chain = chain.chain ++ new_chain.chain
								new_chain.entropy = entropy
								new_chain
							}
					})
					
					// Add the new chains to an accumulator so they can be aggregated by the driver
					for (ch <- new_chains)
						chains_accum += ch
						
				} else {
					
					val last_feature = possible_attributes(0)
					
					val new_chains = last_feature._2.map({
							value => { 
								
								val attrs = sampleRDD.context.broadcast(Array((last_feature._1,value)))
								val value_data = sampleRDD.filter(entry => {attribute_values.value.checkEntryAttributesValues(entry, attrs)})
								val feature_entries = sampleRDD.count
								
								val new_chain = new Chain(feature,value)
								new_chain.chain = chain.chain ++ new_chain.chain
								new_chain.entropy = 0.0
								new_chain.leaf = true
								
								// We assign class using majority of data entries
								val numbers = (0 until classes.size).toArray
								val entries_count = numbers.map(number => {
									(number,Helper.filterByClass(value_data, number).count)
								})
								val max = entries_count.maxBy(_._2)
								new_chain.data_class = max._1
								
								new_chain
							}
					})
						
					// Add the new chains to an accumulator so they can be aggregated by the driver
					for (ch <- new_chains)
						chains_accum += ch

				}
				* */
			
			})
		
			logger.info("Adding new chains...")
			
			// Add new chains discovered to the chainSet. This replaces the chainset with only the last chains. Correct?
			chainSet = dataRDD.context.parallelize(chains_accum.value)
			chains_accum.value.clear
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

}
