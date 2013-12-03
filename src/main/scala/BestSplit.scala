import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream


object BestSplit{

	def bestSplit(inputset:RDD[(Int, Int, Int)], attribset:List[String]): (Set) = {
	
		var rdd_size = inputset.count
		var i_gain = Set[Any]()
		var a = 0
		var f_best = ""
	

		for(a <- 0 until attribset.size) {
			if()
		
		}

	}





}

