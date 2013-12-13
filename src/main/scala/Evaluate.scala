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
 * Object to evaluate the decision tree
 */
object Evaluate {

	val logger = Logger.getLogger(getClass().getName());
	logger.setLevel(Level.DEBUG)
	
	/*
	 * Give the class predicted from the decision tree
	 */
	def predictEntry(	entry: (Int, (Array[Int], Int, Int, Int)), 
						chainSet: RDD[Chain], 
						classes: Array[String]) 
						: Int = {
		
		val attributes = Array("number_words_title","attention", "rating", "engagement")
		val attribute_values = chainSet.context.broadcast(new AttributeValues(attributes))
		logger.debug("chainSet size:" + chainSet.count)
		
		val classes = chainSet.map(chain => {
			
			val attribute_vals = attribute_values.value
			if (attribute_vals.checkEntryAttributesValues(entry, chain.chain.toArray)){
				chain.data_class
			} else {
				-1
			}
		}).filter(clas => clas != -1)
		
		logger.debug("Entry: (" + entry._2._1.mkString(",") + ")")
		logger.debug("Number of coincidences in chains:" + classes.count)
		//logger.debug("Predicted class:" + classes.first)		
		
		/*classes.first*/ 0
	}
}