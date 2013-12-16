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
import org.apache.log4j.Level

/**
 * @author aecc
 * Creation of the decision tree
 */
object Tree {
	
	val logger = Logger.getLogger(getClass().getName());
	logger.setLevel(Level.DEBUG)
	
	/*
	 * Main function for the creation of the tree 
	 * Returns an RDD of Chains
	 */
	def makeDecisionTree(	dataRDD: RDD[(Int, (Array[Int], Int, Int, Int))], 
							attributes: Array[String], 
							classes: Array[String]) 
							: RDD[Chain] = {
		
		logger.debug("Creating the tree...")
		logger.debug("Initial data has " + dataRDD.count)
		
		// Max length of the tree
		val max_depth = attributes.length
		
		val attribute_values = dataRDD.context.broadcast(new AttributeValues(attributes))
		
		// First split to get first best feature
		val ((feature,values),entropies) = BestSplit.bestSplit(dataRDD, 1.0, attribute_values.value.attributes.toArray, attribute_values, classes)
		logger.debug("First Best split is " + feature + " " + values.length)
		
		// Start the tree building. A chain on each value
		var chainSet = dataRDD.context.parallelize(values).map(value => new Chain(feature,value))
		
		// Accumulator for chains
		val chains_accum = dataRDD.context.accumulableCollection[Queue[Chain],Chain](Queue[Chain]())	
			
		// We broadcast all the filtered data in the stream	
		val dataRDD_broadcast = dataRDD.context.broadcast(dataRDD)
		
		var i = 1
		while (i <= max_depth) {

			logger.debug("Creating branches at depth "+i+"...")
			
			logger.debug("ChainSet has length: " + chainSet.count)
				
			chainSet.filter(_.chain.length == i).foreach(chain => {
				
				try {
				if (!chain.leaf) {

					val dataRDD = dataRDD_broadcast.value
					
					val attrs = dataRDD.context.broadcast(chain.getAttributes)
					
					val possible_attributes = chain.getNextPossibleAttributes(attribute_values.value.attributes.toArray) 
					
					// We filter data according to the attributes in the chain
					val sampleRDD = dataRDD.filter(entry => { attribute_values.value.checkEntryAttributesValues(entry, attrs.value) })
					logger.debug("Data in this RDD is of size " + sampleRDD.count)
					sampleRDD.persist
					
					// Find the best split among the attributes remaining
					val ((feature,values),entropies) = BestSplit.bestSplit(sampleRDD, chain.entropy, possible_attributes, attribute_values, classes)
					
					logger.debug("Best split is " + feature)
					
					var j = 0
					
					for (value <- values) {
						
						val new_chain = new Chain(feature,value)
						new_chain.chain = chain.chain ++ new_chain.chain
						new_chain.entropy = entropies(j)
						
						// Entropy 1 means that the value doesn't add information so we discard the growing in this node
						if (entropies(j)==1.0) {
							new_chain.leaf = true
						} else {				
							
							// If this was the last attribute to split
							if (possible_attributes.length==1) {
								
								new_chain.leaf = true
								val attrs = sampleRDD.context.broadcast(Array((feature,value)))
								val value_data = sampleRDD.filter(entry => {attribute_values.value.checkEntryAttributesValues(entry, attrs.value)})
								val feature_entries = sampleRDD.count

								// We assign class using majority of data entries
								val numbers = (0 until classes.size).toArray
								val entries_count = numbers.map(number => {
									(number,Helper.filterByClass(value_data, number).count)
								})
								val max = entries_count.maxBy(_._2)
								if (max._2!=0)
									new_chain.data_class = max._1

							} else {
								// TODO: ?
								// Classify if all the data entries belong to this chain?								
								
							}
							
						}

						// Add the new chains to an accumulator so they can be aggregated by the driver
						chains_accum += new_chain
						j = j+1
	
					}
					
				}
				} catch {
					case e: Exception => {
						println("ERROR")
						e.printStackTrace()
					}
				}
			
			})
		
		
			logger.debug("Adding new chains... Number: " + chains_accum.value.length)
			
			// Add new chains discovered to the chainSet. 
			chainSet ++= dataRDD.context.parallelize(chains_accum.value)
			logger.debug("After adding chains chainSet has length: " + chainSet.count)
			chains_accum.value.clear
			
			i = i+1
		}
		
		// Fiilter to obtain only chains with leaves
		val filtered_chainSet = chainSet.filter(chain => chain.leaf)
		logger.debug("Final size of chainSet: " + filtered_chainSet.count)
		//filtered_chainSet.foreach(chain => println(chain.chain))
		
		filtered_chainSet
		
	}

}
