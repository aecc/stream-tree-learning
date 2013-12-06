import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.DStream
import org.apache.spark.broadcast.Broadcast

/*
 * Main object for filter
 */
object FilterProcess {
	
	/*
	 * Filter the required data from the stream
	 * TODO: change to DStream
	 */
	def filter(	reddits: RDD[String],
				k_parameter: Broadcast[Int] )
				: RDD[(Int, (Array[Int], Int, Int, Int))] = {
	  
		// Count and group by image id, getting only the needed columns, keeping only the oldest post (first) and keep the number of repost
		// image_id = columns(0), unixtime = columns(1), title = columns(3), total_votes = columns(4), score = columns(10), number_of_comments = columns(11), username = columns(12)
		
		val filtered = reddits.map( line => {
				val nline = line + " "
				val splitted = nline.split("\"")
				val nline2 = nline.replaceFirst("\".*\"","")
				val columns = nline2.split(",")	
				if (columns.length!=15) 
					(-1,(Array(0,0,0,0),0,0,0))
				else
					(columns(0).toInt,(Array(
							 if (splitted.length!=1)
								splitted(1).split(" ").length
							 else
								columns(3).split(" ").length,
							 if (columns(4)!="") columns(4).toInt else 0,
							 if (columns(10)!="") columns(10).toInt else 0,
							 if (columns(11)!="") columns(11).toInt else 0),
							 if (columns(1)!="") columns(1).toInt else 0,
							 1,
							 0))
		}).reduceByKey( (l1,l2) => {
				if (l1._2<l2._2) 
					(Array(l1._1(0),l1._1(1),l1._1(2),l1._1(3)),l1._2,l1._3+l2._3,if (l1._3+l2._3<k_parameter.value) 0 else 1) 
				else 
					(Array(l2._1(0),l2._1(1),l2._1(2),l2._1(3)),l2._2,l1._3+l2._3,if (l1._3+l2._3<k_parameter.value) 0 else 1)
		}).filter(entry => if (entry._1 != -1) true else false)
			
		// Data to RETURN in RDD will be in this format now (scala tuple)
		// (image_id, Array(words in title, total_votes, number_of_comments, score),unixtime, number_of_times_reposted, class value))
		// Image_id, Array(number of words, attention, engagement, rating), unixtime, number of times reposted, class value
		// (10003,(Array(8,127,11,10),1321941344,5,1))
		filtered
	  
	}
	
	/*
	 * Return a new RDD with the number of reposts per image_id mixed (added)
	 * TODO or NEVER TODO: delete old image_id from reposts
	 */
	def getRepostsByKey(	filtered: RDD[(Int, (Array[Int], Int, Int, Int))],
							reposts: RDD[(Int,Int)] ) 
							: RDD[(Int,Int)] = {
		// We create another RDD with the number of reposts in this RDD by image_id
		
		val new_reposts = filtered.map {
			case (image_id,(_,_,n_reposts,_)) => {
				(image_id,n_reposts)
			}
		}
		/*
		// We sum the number of reposts of previous data with the new data
		(new_reposts++reposts).reduceByKey(_+_)	*/
		reposts
	}
	
	/*
	 * Sum reposts from the stream with old RDD
	 */
	def mixReposts(	filtered: RDD[(Int, (Array[Int], Int, Int, Int))],
					reposts: RDD[(Int,Int)],
					k_parameter: Broadcast[Int] ) 
					: RDD[(Int, (Array[Int], Int, Int, Int))] = {
		
		// TODO: Maybe there is a better way
		val reposts_formatted = reposts.map {
			case (image_id,n_reposts) => {
				(image_id,(Array[Int](),0,n_reposts,0))
			}
		}
		(filtered++reposts_formatted).reduceByKey((l1,l2) => {
				(	if (l1._1.isEmpty) l2._1 else l1._1,
						if (l1._2 > l2._2) l1._2 else l2._2,
						l1._3 + l2._3,
						if (l1._3+l2._3<k_parameter.value) 0 else 1				
				)
		}).filter{ 
			case (_,(array,_,_,_)) => {
				if (!array.isEmpty) true else false
			}
		}	
		
	}
  
}
