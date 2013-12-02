import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream



object TreeBuilding {

	def growthTree(trainset: DStream[string], attribset:Set):(Set) = {
		
					
			if(attribset.size == 3){// three attributes are using
							
			   val leafnode = new Tuple2()
			   leafnode = classify()	
			} else {
			   val root = new Tuple2()
			   val bestattribute = bestSplit(trainset, attribset) 
			 }
																																					
	}
	
	def classify() {
				
	}
	
	def bestSplit(trainset: DStream[string], attribset:Set):(Set) = {
		



    }																																		


}
