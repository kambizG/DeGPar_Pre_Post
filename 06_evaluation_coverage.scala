//--------------------------------------------------------------------------------------------------------
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
//--------------------------------------------------------------------------------------------------------
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
//--------------------------------------------------------------------------------------------------------
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
//--------------------------------------------------------------------------------------------------------
def getCoverage(tw1:Seq[(String, Int)], wc1:HashMap[String,Int], tc1:HashMap[String,Int] , tw2:Seq[(String, Int)], wc2:HashMap[String,Int], tc2:HashMap[String,Int]): Double ={
  var coverage = 0.0
  for(twl <- tw1){
    for(twr <- tw2){
      var sumTuple = 0
      if(tc1.contains(twl._1 + "," + twr._1)) sumTuple += tc1.get(twl._1 + "," + twr._1).get
      if(tc1.contains(twl._2 + "," + twr._1)) sumTuple += tc1.get(twl._2 + "," + twr._1).get
      if(tc2.contains(twl._1 + "," + twr._1)) sumTuple += tc2.get(twl._1 + "," + twr._1).get
      if(tc2.contains(twl._2 + "," + twr._1)) sumTuple += tc2.get(twl._2 + "," + twr._1).get

      var cwl = 0
      if(wc1.contains(twl._1)) cwl += wc1.get(twl._1).get
      if(wc2.contains(twl._1)) cwl += wc2.get(twl._1).get

      var cwr = 0
      if(wc1.contains(twr._1)) cwr += wc1.get(twr._1).get
      if(wc2.contains(twr._1)) cwr += wc2.get(twr._1).get

      coverage += Math.log((sumTuple+1.0)/(cwl * cwr))
    }}
  return coverage
}
//1111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111
val trial = 0
val numTopSamples = 10

val folder = "/home/kambiz/data/explda/exp_03/"
val docWords = sc.textFile(folder + "doc_info_numbered.txt").map(x => (x.split("\t")(0), x.split("\t")(1)))
val docTopic = sc.textFile(folder + "trial_" + trial +"/docTopic.txt").map(x => (x.split("\t")(0), x.split("\t")(1)))

//val folder = "/home/kambiz/data/btExperiments/exp_03/"
//val docWords = sc.textFile(folder + "doc_info_numbered.txt").map(x => (x.split("\t")(0), x.split("\t")(1)))
//val docTopic = sc.textFile(folder + "trial_" + trial +"/docTopic.txt").map(x => (x.split("\t")(0), x.split("\t")(1)))

//val folder = "/home/kambiz/data/experiments_20%_100/exp_03/"
//val docWords = sc.textFile(folder + "documents.txt").map(x => (x.split("\t")(0), x.split("\t")(1))) // RDD[(String, String)] = (400543,education continues working==girl movie 80s)
//val docTopic = sc.textFile(folder + "trial_" + trial + "/docTopics.txt").map(x => (x.split("\t")(0), x.split("\t")(1)))

//(String, String) = (400543,51)
//(String, String) = (400543,education continues working==girl movie 80s)

val docTopicWords = docTopic.join(docWords)
//(String, (Int, String)) = (400543,(51,education continues working==girl movie 80s))


val initialSet_1 = HashMap.empty[String, Int]
val addToSet_1 = (s:HashMap[String, Int], v: (String, Int)) => s += v
val mergePartitionSets_1 = (p1: HashMap[String, Int], p2: HashMap[String, Int]) => p1 ++= p2

val topWordsPerTopic = getTopNWordsPerTopic(docTopicWords, numTopSamples)

//2222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222
val AllUniqTopWords = topWordsPerTopic.flatMapValues(x => x).map(x => (x._2._1, 1)).reduceByKey((a,b) => a+b)
//(String, Int) = (mayor,2)
val AllWords = docTopicWords.map(x => ((x._1, x._2._1),x._2._2)).flatMapValues(x => x.split(" ")).map(x => (x._2, x._1))
//(String, (String, String)) = (cycle,(1445343,56))
val AllDocTopicsWithTopWords = AllUniqTopWords.join(AllWords).map(x => (x._2._2, x._1)).reduceByKey((a,b) => a + " " + b)
//((String, String), String) = ((11429,28),traffic)

//3333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333
val AllWordPerTopic = AllDocTopicsWithTopWords.map(x => (x._1._2, x._2)).flatMapValues(x => x.split(" ")).map(x => (x, 1)).reduceByKey((a,b) => a+b).map(x => (x._1._1, (x._1._2, x._2)))
val AWPT = AllWordPerTopic.aggregateByKey(initialSet_1)(addToSet_1, mergePartitionSets_1)
//(88,Map(sister -> 5, spent -> 10, delicious -> 1, opening -> 11, seat -> 2, como -> 2,....)

//4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444
val AllTopicTupleCount = AllDocTopicsWithTopWords.map(x => (x._1._2, getAllTouples(x._2))).filter(x => !x._2.equals("")).flatMapValues(x => x.split("\\|")).map(x => (x,1)).reduceByKey((a,b) => a+b)
val AllTopicTuples = AllTopicTupleCount.map(x => (x._1._1, (x._1._2, x._2)))
val ATPT = AllTopicTuples.aggregateByKey(initialSet_1)(addToSet_1, mergePartitionSets_1)
//(88,Map(wonderful,square -> 1, square,john -> 2, spent,listening -> 1,...)

//5555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555555
val topicTable = topWordsPerTopic.join(AWPT).join(ATPT).map(x => (x._1.toInt, x._2))
//(Int, ((Seq[(String, Int)], HashMap[String,Int]), HashMap[String,Int])) 

//6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666
val tupleTable = topicTable.cartesian(topicTable).filter(x => (x._1._1 > x._2._1))
//tupleTable: RDD[((Int, ((Seq[(String, Int)], HashMap[String,Int]), HashMap[String,Int])), (Int, ((Seq[(String, Int)], HashMap[String,Int]), HashMap[String,Int])))]

//7777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777
val TupleCoverage = tupleTable.map(x => ((x._1._1, x._2._1), getCoverage(x._1._2._1._1, x._1._2._1._2, x._1._2._2, x._2._2._1._1, x._2._2._1._2, x._2._2._2)))

val AverageCoverage = TupleCoverage.map(x => ("Coverage", (x._2, 1))).reduceByKey((a,b) => ((a._1 + b._1), (a._2 + b._2))).map(x => (x._1, x._2._1/x._2._2))

AverageCoverage.collect

