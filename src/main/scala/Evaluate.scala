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
 * Object to evaluate the decision tree
 */
object Evaluate {

	def predictEntry(entry: (Int, (Array[Int], Int, Int, Int)), chainSet: RDD[Chain], classes: Array[String]) : Int = {
		
		0
	}
}