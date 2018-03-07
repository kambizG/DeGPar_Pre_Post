//-------------------------------------------------------------------
//Vector Pruning, Normalization and Summarization
//-------------------------------------------------------------------
import org.apache.spark.rdd.RDD

def findThreshold(percent: Double, vector: Array [Double], max: Double ): Double= {
  //Normalize Vector
  var sum = 0.0
  var vec_sorted = new Array [Double] (4000)
  for(i <- 0 to vector.length -1){
    vector(i) = vector(i) / max;
    sum+=vector(i)
    vec_sorted(i) = vector(i)
  }
  // Find n% Threshold
  scala.util.Sorting.quickSort(vec_sorted)
  val vec_sor_desc = vec_sorted.reverse
  var cdf = 0.0
  var threshold = 0.0
  val upperbound = sum * percent
  var i = 0
  while(cdf < upperbound && i < 4000){
    cdf += vec_sor_desc(i)
    threshold = vec_sor_desc(i)
    i+=1
  }
  return threshold
}

//-------------------------------------------------------------------
def prune(vecs:(String, Array[String])): String = {
  var sg = vecs._1 + "\t"
  var vect = vecs._2
  //Create Double Vector
  var vector = new Array [Double] (4000)
  var max = java.lang.Double.MIN_VALUE
  for(i <- 0 to vect.length -1){
    val value = vect(i).toDouble
    if(value.abs > max) max = value.abs
    if(value < 0){
      vector(2000+i) = value.abs
    }else{
      vector(i) = value
    }}
  val threshold = findThreshold(0.3 , vector, max)
  //Prune and summarize vector

  for(i <- 0 to vector.length -1){
    if(vector(i) > threshold){
      sg += i + "," + vector(i) + ";"
    }}
  //val res = correl(word, vector)
  return sg
}

//-------------------------------------------------------------------
val dir = "target_dir/"
val vecString = sc.textFile(folder + "english-prod-2016.txt").map(x => (x.split("\t")(0), x.split("\t")(1).split(",")))
val vectors_normalize_prunned_summarized = vecString.map(x => prune(x)).filter(x => x.split("\t").length > 1)
val file_pruned_vectors = dir + "vectors/vec_30%"
vectors_normalize_prunned_summarized.saveAsTextFile(file_pruned_vectors)

