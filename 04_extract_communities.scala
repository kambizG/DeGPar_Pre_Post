//---------------------------------------------------------------------------------------------------
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
//---------------------------------------------------------------------------------------------------
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
def costumMax(v1: (String, Double), v2: (String, Double)): (String, Double) ={
  if(v1._2 > v2._2)
    return v1
  else
    return v2
}
//---------------------------------------------------------------------------------------------------
def getCorrelationSubGraph(v1: HashMap[String, Double], v2: HashMap[String, Double]): HashMap[String, Double] = {
  val map = HashMap.empty[Int, Double]
  for(key <- v1.keys){
    if(v2.contains(key)){
      map.put(key.toInt, v1.get(key).get * v2.get(key).get)
    }}
  val res = HashMap.empty[String,Double]
  for(k1 <- map.keys){
    for(k2 <- map.keys){
      if(k1 < k2)
        res.put(k1 + "-" + k2 , map.get(k1).get * map.get(k2).get)
    }}
  return res
}

//---------------------------------------------------------------------------------------------------
def getCorrelationSubGraphWithCosine(v1: HashMap[String, Double], v2: HashMap[String, Double]): HashMap[String, Double] = {
  val map = HashMap.empty[Int, Double]

  var dotProduct = 0.0
  var normA = 0.0
  var normB = 0.0

  for(key <- v1.keys){
    if(v2.contains(key)){
      map.put(key.toInt, v1.get(key).get * v2.get(key).get)
      dotProduct += v1.get(key).get * v2.get(key).get
      normA += Math.pow(v1.get(key).get, 2);
      normB += Math.pow(v2.get(key).get, 2);
    }}

  val res = HashMap.empty[String,Double]
  val cosine = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB))
  if(cosine < 0.5 || map.size < 2){
    return res
  }else{
    for(k1 <- map.keys){
      for(k2 <- map.keys){
        if(k1 < k2)
          res.put(k1 + "-" + k2 , map.get(k1).get * map.get(k2).get)
      }}
    return res
  }
}
//---------------------------------------------------------------------------------------------------
def extractComm(index: Int, i: Int, folder: String, docString: org.apache.spark.rdd.RDD[String], vectors:org.apache.spark.rdd.RDD[(String, scala.collection.mutable.HashMap[String,Double])], unit: Int ) {
  println("trial: " + i + "\t Index: " + (index+1))
  val documents = sc.parallelize(docString.take((index + 1) * unit).drop(index * unit))
  val docBigrams = documents.map(x => (x.split("\t")(0), getAllBigrams(x.split("\t")(1)))).flatMapValues(x => x.split("\\|")).map(x => (x._2, x._1))
  //val bigrams = documents.map(x => x.split("\t")(1)).flatMap(x => x.split(" ").sliding(2))
  //val bigAll = bigrams.map(x => ((x(0), x(1)),x)).reduceByKey((a,b) => a).flatMapValues(x => x).map(x => (x._2, x._1))
  //val bigrams = documents.map(x => getAllBigrams(x.split("\t")(1))).flatMap(x => x.split("\\|"))
  val bigAll = docBigrams.map(x => (x._1,x._1)).reduceByKey((a,b) => a).flatMapValues(x => x.split(",")).map(x => (x._2, x._1))
  val bigVecs = vectors.join(bigAll).map(x => (((x._2)_2) , (x._2)_1))
  bigVecs.flatMapValues(x => x).map(x => ((x._1, x._2._1), x._2._2)).reduceByKey((a,b) => a * b)
  //------
  val subgraphs = bigVecs.reduceByKey((a,b) => getCorrelationSubGraphWithCosine(a,b))
  val temp = subgraphs.flatMapValues(x => x).filter(x => ((x._2)_1).contains("-")) //((String, String), (String, Double)) = ((wars,minute),(293-2176,0.003499998343683394))
  //val docBigrams = documents.map(x => (x.split("\t")(0), x.split("\t")(1))).flatMapValues(x => x.split(" ").sliding(2)).map(x => ((x._2(0), x._2(1)),x._1)) //((String, String), String) = ((education,continues),400543)
  //------
  val temp2 = docBigrams.join(temp).map(x => (x._2._2._1, (x._2._1, x._2._2._2))) //(String, (String, Double)) = (293-2176,(10820766,0.003499998343683394))
  //------
  val edgeList = sc.textFile(folder + "trial_" + i + "/edgeList.csv").map(x => (x.split("\t")(0) + "-" + x.split("\t")(1), x.split("\t")(2)))
  //------
  val temp3 = edgeList.join(temp2) // (String, (String, (String, Double))) = (3085-3099,(80,(874522,0.00843882424489076)))
  //------
  val docTopic = temp3.map(x => ((x._2._2._1, x._2._1), x._2._2._2)).reduceByKey((a,b) => a+b) //((String, String), Double) = ((8061520,35),0.006768723253035296)
  val docTotal = docTopic.map(x => ( (x._1)_1 , x._2)).reduceByKey((a, b) => a+b) //(String, Double) = (11778822,0.37464679535945233) 
  val docTopicRatios = docTopic.map(x => ((x._1)_1 , ( (x._1)_2, x._2))).join(docTotal).map(x => (x._1 , (((x._2)_1)_1 , (((x._2)_1)_2)/((x._2)_2) ) ))
  val docDominantTopic = docTopicRatios.reduceByKey((a, b) => costumMax(a,b)).map(x => (x._1 + "\t" + (x._2)._1))
  docDominantTopic.saveAsTextFile(folder + "trial_" + i + "/docTopic_" + (unit * (index + 1)))
}
//---------------------------------------------------------------------------------------------------
def calculateAll(trial: Int, index: Array[Int], folder: String, docString: org.apache.spark.rdd.RDD[String], vectors:org.apache.spark.rdd.RDD[(String, scala.collection.mutable.HashMap[String,Double])], unit: Int ){
  index.foreach(extractComm(_, trial, folder, docString, vectors, unit))
}

def readVectors(docString: RDD[String]): RDD[(String, HashMap[String, Double])]= {
  val wordsUniq = docString.map(x => x.split("\t")(1)).flatMap(x => x.split(" ")).groupBy(x => x)
  val vecAll = sc.textFile("/home/kambiz/data/vectors/vec_20%.txt").filter(x => x.split("\t").length > 1).map(x => (x.split("\t")(0).replaceAll(" ", "=="), x.split("\t")(1))).reduceByKey((a,b) => a)
  val vecFlat = vecAll.join(wordsUniq).map(x => (x._1, (x._2)_1)).flatMapValues(x => x.split(";")).map(x => (x._1, (x._2.split(",")(0), x._2.split(",")(1).toDouble)))
  val initialSet = scala.collection.mutable.HashMap.empty[String, Double]
  val addToSet = (s:scala.collection.mutable.HashMap[String, Double], v: (String, Double)) => s += v
  val mergePartitionSets = (p1: scala.collection.mutable.HashMap[String, Double], p2: scala.collection.mutable.HashMap[String, Double]) => p1 ++= p2
  val vectors = vecFlat.aggregateByKey(initialSet)(addToSet, mergePartitionSets) //[W, V]
  return vectors
}

val folder = "/home/kambiz/data/experiments_20%_100/exp_04/"
val docString = sc.textFile(folder + "documents.txt")
val vectors = readVectors(docString)

val index = Array(0,1,2,3,4,5)
//val trial = Array(0,1,2,3,4,5,6,7,8,9)
calculateAll(9, index, folder, docString, vectors, 500000)


val index = Array(6,7,8,9,10)
calculateAll(9, index, folder, docString, vectors, 500000)


for i in {0..9}; do cat trial_$i/docTopic_*/part-00* >> trial_$i/docTopics.txt


