//-----------------------------------------------------------------------------
// Calculate Coherency
//-----------------------------------------------------------------------------
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap

def getAllTouples(document: String): String = {
  val words = document.split(" ")
  if(words.length < 2) return ""
  var res = ""
  for(i <- 0 to words.length-2){
    for(j <- i+1 to words.length-1){
      res += words(i) + "," + words(j) + "|"
    }}
  return res
}
//-----------------------------------------------------------------------------
def getTopicCoherency(topWords: Seq[(String, Int)], topTupes:HashMap[String, Int]): Double = {
  var totalCoherency = 0.0
  for(i <- 0 to topWords.length - 2){
    val w1 = topWords(i)._1
    for(j <- i+1 to topWords.length - 1){
      val w2 = topWords(j)._1
      var sum = 0
      if(topTupes.contains(w1 + "," + w2)) sum += topTupes.get(w1 + "," + w2).get
      if(topTupes.contains(w2 + "," + w1)) sum += topTupes.get(w2 + "," + w1).get
      totalCoherency += Math.log((sum + 1.0) / topWords(i)._2)
    }}
  return totalCoherency
}
//-----------------------------------------------------------------------------
def getTopNWordsPerTopic(docTopicWords: RDD[(String, (String, String))], N: Int): RDD[(String, Seq[(String, Int)])] = {
  val topicWordCount = docTopicWords.map(_._2).flatMapValues(x => x.split(" ")).map(x => (x,1)).reduceByKey((a,b) => a+b)
  //RDD[((String, String), Int)] = ((93,national),1) = [Topic, Word, Count]
  val initialSet = HashMap.empty[String, Int]
  val addToSet = (s:HashMap[String, Int], v: (String, Int)) => s += v
  val mergePartitionSets = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2
  val topicWordList = topicWordCount.map(x => ((x._1)_1, ((x._1)_2, x._2))).aggregateByKey(initialSet)(addToSet, mergePartitionSets)
  //RDD[(String, scala.collection.mutable.HashMap[String,Int])] = (88,Map(hardest -> 1, simmons -> 1, delicious -> 1, ...))
  val sortedTopicWordList = topicWordList.map(x => (x._1, (x._2).toSeq.sortWith(_._2 > _._2)))
  //(String, Seq[(String, Int)]) = (Topic, Seq[(word, count)]) = (55 ,[(easily,1), (stars,1), (blue==sky,1), ...])
  val topWordsPerTopic = sortedTopicWordList.map(x => (x._1, (x._2).take(N)))
  return topWordsPerTopic
}
//-----------------------------------------------------------------------------
def getTopTuplesPerTopic(docTopicWords: RDD[(String, (String, String))], topWordsPerTopic: RDD[(String, Seq[(String, Int)])]): RDD[(String, HashMap[String, Int])] = {
  val topWordsPerTopicFlat = topWordsPerTopic.flatMapValues(x => x).map(x => ((x._1, x._2._1), x._2._2))
  //((String, String), Int) = ((88,garden),6)
  val docWordsPerTopic = docTopicWords.map(x => ((x._1, x._2._1), x._2._2)).flatMapValues(x => x.split(" ")).map(x => ((x._1._2, x._2), x._1._1)) // [ (Topic, Word), Doc]
  //((String, String), String) = ((51,education),400543)
  val topTopicDocumentWords = topWordsPerTopicFlat.join(docWordsPerTopic).map(x => ((x._1._1, x._2._2), x._1._2)).reduceByKey((a,b) => a + " " + b)
  //((String, String), String) = ((98,11939928),how==many)
  val topDocTopicWords = topTopicDocumentWords.map(x => (x._1._1, getAllTouples(x._2))).flatMapValues(x => x.split("\\|")).filter(x => !x._2.equals("")).map(x => (x,1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, (x._1._2, x._2)))
  //-------
  val initialSet_1 = HashMap.empty[String, Int]
  val addToSet_1 = (s:HashMap[String, Int], v: (String, Int)) => s += v
  val mergePartitionSets_1 = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2
  val topTuplesPerTopic = topDocTopicWords.aggregateByKey(initialSet_1)(addToSet_1, mergePartitionSets_1) //[W, V]
  //(String, scala.collection.mutable.HashMap[String,Int]) = (88,Map(of==the==house-the==house -> 2, meal-eve -> 1, fashion-italian -> 2, fashion-italian==fashion -> 2))
  return topTuplesPerTopic
}
//-----------------------------------------------------------------------------

def calculateCoherency(folder: String, trial: Int, numTopSamples: Int, docWords: RDD[(String, String)]): Double ={
  val docTopic = sc.textFile(folder + "/trial_" + trial + "/docTopics.txt").map(x => (x.split("\t")(0), x.split("\t")(1)))
  //(String, String) = (400543,51)

  //(String, String) = (400543,education continues working==girl movie 80s)
  val docTopicWords = docTopic.join(docWords)
  //(String, (String, String)) = (400543,(51,education continues working==girl movie 80s))

  val topWordsPerTopic = getTopNWordsPerTopic(docTopicWords, numTopSamples)
  topWordsPerTopic.saveAsTextFile(folder + "/trial_" + trial + "/topWords")

  val topTuplesPerTopic = getTopTuplesPerTopic(docTopicWords, topWordsPerTopic)
  //topTuplesPerTopic.saveAsTextFile(folder + "/trial_" + trial + "/topTuples")

  val toipcCoherencies = topWordsPerTopic.join(topTuplesPerTopic).map(x => (x._1, getTopicCoherency(x._2._1, x._2._2))).map(x => ("Coherency", (x._2, 1))).reduceByKey((a,b) => ((a._1 + b._1), (a._2 + b._2))).map(x => (x._1, x._2._1/x._2._2))
  toipcCoherencies.first._2
}

val trial = 4
val numTopSamples = 10
val folder = "/home/kambiz/data/experiments_20%_100/exp_03/"
val docWords = sc.textFile(folder + "documents.txt").map(x => (x.split("\t")(0), x.split("\t")(1))) // RDD[(String, String)] = (400543,education continues working==girl movie 80s)

val trial = Array(0,1,2,3,4,5,6,7,8,9)
trial.foreach(x => println(calculateCoherency(folder, x, 5, docWords)))
trial.foreach(x => println(calculateCoherency(folder, x, 10, docWords)))
trial.foreach(x => println(calculateCoherency(folder, x, 20, docWords)))




val temp = topicWordCount.map(x => (x._1._1, x._2)).reduceByKey((a,b) => a+b)
val temp2 = temp.join(topWordsPerTopic).map(x => ((x._1 , x._2._1),x._2._2)).flatMapValues(x => x).map(x => (x._1._1,(x._2._1, x._2._2.toDouble/x._1._2)))
val temp3 = temp2.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
temp3.saveAsTextFile(folder + "/trial_" + trial + "/topWords_prob")

