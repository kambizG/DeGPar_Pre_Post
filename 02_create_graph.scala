import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD

def getAllBigrams(document: String): String = {
  var words = document.trim.split(" ")
  var result = ""
  for(i <- 0 to words.length - 2){
    for(j <- i+1 to words.length - 1){
      if(!words(i).equals(words(j))){
        result += words(i) + "," + words(j) + "|"
      }}}
  return result
}
//---------------------------------------------------------------------------------------------------
def getCorrelationSubGraphWithCosine(v1: (Int, HashMap[String, Double]), v2: (Int, HashMap[String, Double])): (Int, HashMap[String, Double]) = {
  val c = v1._1
  val m1 = v1._2
  val m2 = v2._2
  val map = HashMap.empty[Int, Double]

  var dotProduct = 0.0
  var normA = 0.0
  var normB = 0.0

  for(key <- m1.keys){
    if(m2.contains(key)){
      map.put(key.toInt, m1.get(key).get * m2.get(key).get)
      dotProduct += m1.get(key).get * m2.get(key).get
      normA += Math.pow(m1.get(key).get, 2);
      normB += Math.pow(m2.get(key).get, 2);
    }}

  val cosine = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB))
  if(cosine < 0.5 || map.size < 2){
    return (-1, null)
  }else{
    val res = HashMap.empty[String,Double]
    for(k1 <- map.keys){
      for(k2 <- map.keys){
        if(k1 < k2)
          res.put(k1 + "-" + k2 , map.get(k1).get * map.get(k2).get * c)
      }}
    return (0, res)
  }
}
//---------------------------------------------------------------------------------------------------
def extractSubGraph(index: Int, folder: String, docString: RDD[String], vectors: RDD[(String, HashMap[String,Double])], unit: Int): String ={
  var t0 = System.nanoTime()
  println("From: \t" + (index * unit) + "\t To: \t" + ((index + 1) * unit))
  val documents = sc.parallelize(docString.take(unit * (index + 1)).drop(unit * index)).map(x => x.split("\t")(1))
  val bigramsUniq = documents.map(x => (getAllBigrams(x))).flatMap(x => x.split("\\|")).map(x => (x,1)).reduceByKey((a,b) => a+b) //[(w1,w2),C]
  val bigAll = bigramsUniq.map(x => (x,x._1)).flatMapValues(x => x.split(",")).map(x => (x._2, x._1))
  val bigrams = vectors.join(bigAll).map(x => (((x._2)_2)_1 , ((((x._2)_2)_2), (x._2)_1)))	//[k, (C, V)]
  val subgraphs = bigrams.reduceByKey((a,b) => getCorrelationSubGraphWithCosine(a,b)).filter(x => x._2._1 == 0).flatMap(x => x._2._2).reduceByKey((a,b) => a + b).map(x => x._1 + "\t" + x._2)
  subgraphs.saveAsTextFile(folder + "graph_cosine/RDG00_"+ (index + 1) * unit +".gr")
  return ("Elapsed time: " + (System.nanoTime() - t0)/1000000000)
}
//---------------------------------------------------------------------------------------------------
// Read Vectors
//---------------------------------------------------------------------------------------------------
def readVectors(docString: RDD[String]): RDD[(String, HashMap[String, Double])]= {
  val wordsUniq = docString.map(x => x.split("\t")(1)).flatMap(x => x.split(" ")).groupBy(x => x)
  val vecAll = sc.textFile("/home/kambiz/data/vectors/vec_20%.txt").filter(x => x.split("\t").length > 1).map(x => (x.split("\t")(0).replaceAll(" ", "=="), x.split("\t")(1))).reduceByKey((a,b) => a)
  val vecFlat = vecAll.join(wordsUniq).map(x => (x._1, (x._2)_1)).flatMapValues(x => x.split(";")).map(x => (x._1, (x._2.split(",")(0), x._2.split(",")(1).toDouble)))
  val initialSet = HashMap.empty[String, Double]
  val addToSet = (s:scala.collection.mutable.HashMap[String, Double], v: (String, Double)) => s += v
  val mergePartitionSets = (p1: scala.collection.mutable.HashMap[String, Double], p2: scala.collection.mutable.HashMap[String, Double]) => p1 ++= p2
  val vectors = vecFlat.aggregateByKey(initialSet)(addToSet, mergePartitionSets) //[W, V]
  return vectors
}


val folder = "/home/kambiz/data/experiments_20%_100/exp_04/"
val docString = sc.textFile(folder + "documents.txt")
val vectors = readVectors(docString)
val index = Array(7)
index.foreach(x => println(extractSubGraph(x, folder, docString, vectors, 500000)))



val folder = "/home/kambiz/data/experiments_20%_100/exp_00/"
val docString = sc.textFile(folder + "documents.txt")
val vectors = readVectors(docString)
println(extractSubGraph(0, folder, docString, vectors, 10000))

val folder = "/home/kambiz/data/experiments_20%_100/exp_01/"
val docString = sc.textFile(folder + "documents.txt")
val vectors = readVectors(docString)
println(extractSubGraph(0, folder, docString, vectors, 100000))

val folder = "/home/kambiz/data/experiments_20%_100/exp_02/"
val docString = sc.textFile(folder + "documents.txt")
val vectors = readVectors(docString)
val index = Array(0,1)
index.foreach(x => println(extractSubGraph(x, folder, docString, vectors, 500000)))

val folder = "/home/kambiz/data/experiments_20%_100/exp_03/"
val docString = sc.textFile(folder + "documents.txt")
val vectors = readVectors(docString)
val index = Array(0,1,2,3,4,5,6)
index.foreach(x => println(extractSubGraph(x, folder, docString, vectors, 500000)))









