import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap


object BestSplit{

	def bestSplit(inputset: RDD[(Int, (Array[Int], Int, Int, Int))], attribset: Array[String], classes: Array[String]): 
	(java.lang.String, Array[Int=>Boolean]) = {
	
		for(a <- 0 until attribset.size) {
		
			val new_rdd = mapSamples(inputset, attribset(a)) 
		
			if(new_rdd.count() > 0){

				val entropyAfterSplit = new_rdd.map(inputset=>{Entropy.calculateEntrophy(classes, inputset)})
				
			}
		}
		
	}
	

	def mapSamples(inputset: RDD[(Int, (Array[Int], Int, Int, Int))], feature: java.lang.String): RDD[(java.lang.String, (Array[Int], Int))] = {
	
		val featureAndValue = inputset.map{

			case(_,(Array(notitle,_,_,_),_,_,classType))=> {
				(feature, notitle,classType)
			}
		
			case(_,(Array(_,attention,_,_),_,_,classType)) => {
				(feature, attention, classType)
			}
			
			case(_,(Array(_,_,engagement,_),_,_,classType)) => {
				(feature,  engagement, classType)
			}
			
			case(_,(Array(_,_,_,rating),_,_,classType)) => {
				(feature, rating, classType)
			}
		}

			featureAndValue.groupBy()//need to group by feature
	
		
	}

}
