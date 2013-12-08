import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import scala.Array.canBuildFrom
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.AccumulatorParam
import org.apache.spark.AccumulableParam

 /* @author aecc
 * A chain of features and its values, represented as a linked list
 */

class Chain(feature: String, value: Int => Boolean) {
	
	var chain = List[(String,Int => Boolean)]((feature,value))	
	var entropy = 0.0
	
	/*
	 * Get the names of the attributes in the chain
	 */
	def getAttributesNames(): Array[String] = {
		val chain_array = chain.toArray
		chain_array.map {
			case (feat, vals) => feat
		}
	}
	
	/*
	 * Get the attribute array of the chain 
	 */
	def getAttributes(): Array[(String, Int => Boolean)] = {
		chain.toArray
	}
	
	/*
	 * Obtains all the possible attributes of the next split according to the attributes already used in the chain
	 */
	def getNextPossibleAttributes(total_attrs: Array[(String, Array[Int => Boolean])]) : Array[(String, Array[Int => Boolean])] = {
		
		var result_array = Array[(String, Array[Int => Boolean])]()
		val chain_array = chain.toArray
		
		for (attr <- total_attrs) {
			var is = false
			for (attr2 <- chain_array) {
				if (attr._1.equals(attr2._1)) is = true
			}
			if (!is) 
				result_array = result_array :+ attr
		}
		result_array
	}
	
}