import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream

/*
 * Main object for filter
 */
object FilterProcess {
	
	def filter(reddits: DStream[String]): DStream[(Int, (java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, Int))] = {
	  
		// Count and group by image id, getting only the needed columns, keeping only the oldest post (first) and keep the number of repost
		// image_id = columns(0), unixtime = columns(1), title = columns(3), total_votes = columns(4), number_of_comments = columns(11), username = columns(12)
			
		val filtered = reddits.map( line => {
				val nline = line + " "
				val splitted = nline.split("\"")
				val nline2 = nline.replaceFirst("\".*\"","")
				val columns = nline2.split(",")	
				if (columns.length!=15) 
					(-1,("","","","","",0))
				else
					(columns(0).toInt,(
							 columns(1),
							 if (splitted.length!=1)
								splitted(1)
							 else
								columns(3),
							 columns(4),
							 columns(11),
							 columns(12).trim(),
							 1))
		}).reduceByKey( (l1,l2) => {
				if (l1._1<l2._1) 
					(l1._1,l1._2,l1._3,l1._4,l1._5,l1._6+l2._6) 
				else 
					(l2._1,l2._2,l2._3,l2._4,l2._5,l1._6+l2._6)
		})
	
		// We need an action to begin the process
		filtered.print()
		
		// Data to RETURN will be in this format now (scala tuple)
		// (image_id, (unixtime, title, total_votes, number_of_comments, username, number_of_times_reposted))
		// (10003,(1321941344,and who says technology has to be boring?,127,11,irishjack777,5))
		filtered
	  
	}
  
}
