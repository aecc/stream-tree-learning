/**
 * @author aecc
 * Class to represent a set of classes of the decision tree
 * They should be represented by numbers asigned to the string here available
 */
class ClassesSet(names: Array[String]) {

	private val this.names = names
	val size = names.length;
	
	def get(i: Int): String = {
		this.names(i)
	}
	
}