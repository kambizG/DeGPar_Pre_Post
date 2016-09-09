//-------------------------------------------------------------------
//Cleaning - new
//-------------------------------------------------------------------
// For each documents we extract all the compound including (bigrams and trigrams). We hope that useless bigrams will be removed further in the process due to high or low frequency.

// In This new cleaner we added two sections:
// 1 = remove compounds that conatin stop-words like this==is I==am, ...
// 2 = in each document we remove words that also partitipate in compuonds
// 3 = in each document we remove small compounds that also participate in large compounds
// 4 = remove 100 highest frequent words
// 5 = remove all words with frequency lower than 10

def getTokens(document: String): String = {
  var words = document.trim.toLowerCase().replaceAll("[!\"$%&'*+,./:;<=>?\\[\\]^`{\\|}~()]", " ").replaceAll("http", "").replaceAll("\\s+", " ").split("\\s")
  if(words.length < 2) return ""
  var res = ""
  var i = 0
  while(i < words.length){
    if(i< words.length-2)
      res += words(i) + "==" + words(i+1) + "==" + words(i+2) + "|"
    if(i < words.length - 1)
      res += words(i) + "==" + words(i+1) + "|"
    res += words(i) + "|"
    i += 1;
  }
  return res
}

val vecAll = sc.textFile("/home/kambiz/data/vectors/vec_30%.txt").filter(x => x.split("\t").length > 1).map(x => (x.split("\t")(0).replaceAll(" ", "=="), 1)).reduceByKey((a,b) => a)
val freqStopWords = sc.textFile("/home/kambiz/data/freqStopWords.txt").map(x => (x,1))
val valid_tokens = vecAll.subtractByKey(freqStopWords).map(x => x._1).filter(x => x.length > 2 && (x.contains("==") || x.length < 25)).map(x => (x,1))
//val tokens = sc.parallelize(sc.textFile("/home/kambiz/data/02_tw_lo_har_freq/tw_lo_har.txt").take(10000)).filter(x => x.split("\t").length > 1).map(x => (x.split("\t")(0), getTokens(x.split("\t")(1)))).flatMapValues(x => x.split("\\|")).map(x => (x._2, x._1))

// 1 = remove compounds that conatin stop-words like this==is I==am, ...
val temp = valid_tokens.filter(x => x._1.contains("==")).map(x => (x._1, x._1)).flatMapValues(x => x.split("==")).map(x => (x._2, x._1))
val temp2 = temp.subtractByKey(freqStopWords).map(x => (x._2, x._1)).reduceByKey((a,b) => a + "==" + b).filter(x => x._2.contains("==")).map(x => (x._1, 1))
val valid_tokens_comps = valid_tokens.filter(x => !x._1.contains("==")).union(temp2)

val tokens = sc.textFile("/home/kambiz/data/02_tw_lo_har_freq_100/tw_lo_har.txt").filter(x => x.split("\t").length > 1).map(x => (x.split("\t")(0), getTokens(x.split("\t")(1)))).flatMapValues(x => x.split("\\|")).map(x => (x._2, x._1))
val valid_words = valid_tokens_comps.join(tokens).map(x => (x._1, x._2._2))
val uniq_valid_words = valid_words.map(x => (x,1)).reduceByKey((a,b) => a+b).map(x => x._1) //(String, String) = (smashed,12641966)

// 2 = in each document we remove words that also partitipate in compuonds
// 3 = in each document we remove small compounds that also participate in large compounds
def getLargerCom(a: String, b:String): String = {
  if(a.split("==").length > b.split("==").length)
    return a
  else
    return b
}
val temp3 = uniq_valid_words.map(x => (x,x._1)).flatMapValues(x => x.split("==")).map(x => ((x._2, x._1._2), x._1._1)) //((W,D), C|W)
val temp4 = temp3.reduceByKey((a,b) => getLargerCom(a,b))
val temp5 = temp4.map(x => ((x._2, x._1._2), 1)).reduceByKey((a,b) => a).map(x => x._1)

// 4 = remove 100 highest frequent words
val frequentWrods = temp5.map(x=> (x._1,1)).reduceByKey((a,b) => a+b).map(x => (x._2, x._1)).sortByKey(false)
val firstT = sc.parallelize(frequentWrods.take(100)).map(x => (x._2, x._1))
val final_valid_words = temp5.subtractByKey(firstT)

// 5 = remove all words with frequency lower than 10
val wc = final_valid_words.map(x=> (x._1,1)).reduceByKey((a,b) => a+b).filter(x=> x._2 > 10)
val freqstopless = wc.join(final_valid_words) //(String, (Int, String)) = (2014,(4,93)) = [W, (C, D)]

val documents = freqstopless.map(x => (((x._2)_2), x._1)).reduceByKey((a,b) => a + " " + b).filter(x => x._2.split(" ").size > 2).map(x => x._1 + "\t" + x._2)
documents.saveAsTextFile("/home/kambiz/data/RDC002_30%_100.dc")




//---------------------------------------------------------------------------
//---------------------------------------------------------------------------



//---------------------------------------------------------------------------
// Read Vectors
//---------------------------------------------------------------------------
// Uniq vectors. Compunds contain ==.
val vectors = sc.textFile("/home/kambiz/data/vec_40%.txt").map(x => (x.split("\t")(0).replaceAll(" ", "=="), 1)).reduceByKey((a,b) => a)
//val sig_unig = vectors.filter(x => !x._1.contains("=="))
//val sig_comp = vectors.filter(x => x._1.contains("=="))

//val wordsUniq = sc.textFile("/home/kambiz/data/02_tw_lo_har_freq/tw_lo_har.txt").filter(x => x.split("\t").length > 1).flatMap(x => (x.split("\t")(1)).split(" ")).map(x => (x, 1)).reduceByKey((a,b) => a)
//val vecValid = vecAll.join(wordsUniq)
//---------------------------------------------------------------------------
// Clean Documents and remove StopWords
//---------------------------------------------------------------------------
//val tokens = sc.parallelize(sc.textFile("/home/kambiz/data/02_tw_lo_har_freq/tw_lo_har.txt").take(100000))
//val freqStopWords = sc.textFile("/home/kambiz/data/freqStopWords.txt").collect
//val tokens = sc.parallelize(sc.textFile("/home/kambiz/data/02_tw_lo_har_freq/tw_lo_har.txt").take(1000000)).filter(x => x.split("\t").length > 1).map(x => (x.split("\t")(0), getTokens(x.split("\t")(1), freqStopWords)))
//val tokens_clean = tokens.flatMapValues(x => x.split("\\|")).filter(x => x._2.length > 2 && (x._2.contains("==") || x._2.length < 25)).map(x => (x._2, x._1))
// val compounds = sig_comp.join(tokens_clean).map(x => ((x._2)_2 , x._1)) //[(String, String)] = [(comp, D)]
// val unigrams = sig_unig.join(tokens_clean).map(x => ((x._2)_2 , x._1))	//[(String, String)] = [(unig, D)]
//tokens_clean.persist()
//val tokens_clean = t2.subtractByKey(freqStopWords) //(String, String) = (means==91==nationalities,307060) = [W, D]
//val tokens_clean = freqStopWords.subtractByKey(t2)
//val tokens_clean = freqStopWords.union(t2)
//tokens_clean.persist
//val tokens_nonStop = tokens_clean.reduceByKey((a,b) => a+ "|" + b)
//val tokens_nonStop_2 = tokens_nonStop..filter(x => !x._2.contains("-1")).flatMapValues(x => x.split("\\|"))
//compounds.persist()
//---------------------------------------------------------------------------
//Remove unigrams shared with compunds (remove redundancy)
//---------------------------------------------------------------------------

//val compSplit = compounds.flatMapValues(x => x.split("=="))
//val unigNonShared = unigrams.subtractByKey(compSplit)
//wordsAll.persist()
//val wordsAll = unigNonShared.union(compounds).map(x => (x._2, x._1))
//---------------------------------------------------------------------------
// Remove Low Frequent Words
//---------------------------------------------------------------------------
val wc = wordsAll.map(x=> (x._1,1)).reduceByKey((a,b) => a+b).filter(x=> x._2 > 10)
val freqstopless = wc.join(wordsAll) //(String, (Int, String)) = (2014,(4,93)) = [W, (C, D)]
val documents = freqstopless.map(x => (((x._2)_2), x._1)).reduceByKey((a,b) => a + " " + b).filter(x => x._2.split(" ").size > 1).map(x => x._1 + "\t" + x._2)
documents.saveAsTextFile("/home/kambiz/data/RDC002.dc")

//################################################################################################################
//-------------------------------------------------------------------
//Cleaning - old
//-------------------------------------------------------------------
def getTokens(document: String, sw: Array[String]): String = {
  val parts = document.split("\t", 2)
  if(parts.length < 2) return ""
  val dn = parts(0).trim.toInt
  //Remove Punctuations and http prefix
  var words = parts(1).trim.toLowerCase().replaceAll("[!\"$%&'*+,./:;<=>?\\[\\]^`{\\|}~()]", " ").replaceAll("http", "").replaceAll("\\s+", " ").split("\\s")
  if(words.length < 2) return ""
  var list=""
  for(i <- 0 to words.length - 2){
    if(words(i).length > 3 && words(i).length < 25 && !sw.contains(words(i))) list += words(i) + "\t" + dn + "|"
    if(i < words.length - 2){
      list += words(i) + "==" + words(i+1) + "==" + words(i+2) + "\t" + dn + "|"
    }
    val bigram = words(i) + "==" + words(i+1)
    if(!sw.contains(bigram)){
      list += bigram + "\t" + dn + "|"
    }}
  var lastWord = words(words.length-1)
  if(!sw.contains(lastWord)){
    if(lastWord.length > 3 && lastWord.length < 25) {
      list += lastWord + "\t" + dn + "|"
    }}
  return list
}
//---------------------------------------------------------------------------
// Read Vectors
//---------------------------------------------------------------------------
val folder = "/home/kambiz/data/"
// Uniq vectors. Compunds contain ==.
val vectors = sc.textFile(folder + "vec_40%.txt").map(x => (x.split("\t")(0).replaceAll(" ", "=="), 1)).reduceByKey((a,b) => a)
val sig_unig = vectors.filter(x => !x._1.contains("=="))
val sig_comp = vectors.filter(x => x._1.contains("=="))
//---------------------------------------------------------------------------
// Clean Documents and remove StopWords
//---------------------------------------------------------------------------
//val StopWords = sc.textFile(folder + "longstoplist.txt")
val freqStopWords = sc.textFile(folder + "freqStopWords.txt")
val sw = freqStopWords.collect
//val tokens = sc.textFile(folder + "TREC_Tweets_2011.txt").map(x => getTokens(x, sw)).filter(_.nonEmpty).flatMap(_.split("\\|"))
val tokens = sc.textFile(folder + "02_tw_lo_har_freq/tw_lo_har.txt").map(x => getTokens(x, sw)).filter(_.nonEmpty).flatMap(_.split("\\|"))  //RDD[String] = (hammersmith	1) = (W 	D)
val tokens_uniq = tokens.map(x => (x,1)).reduceByKey((a,b) => a+b).map(x => (x._1.split("\t")(0),x._1.split("\t")(1)))
val compounds = sig_comp.join(tokens_uniq).map(x => (x._1, (x._2)_2)) //[(String, String)] = [(comp, D)]
val unigrams = sig_unig.join(tokens_uniq).map(x => (x._1, (x._2)_2))	//[(String, String)] = [(unig, D)]
//---------------------------------------------------------------------------
//Remove unigrams shared with compunds (remove redundancy)
//---------------------------------------------------------------------------
val compSplit = compounds.map(x => x._1.replaceAll("==", "\t" + ((x._2) + "|")) + "\t" + x._2).flatMap(_.split("\\|")).map(x => (x,1))	//[(comp ,D)] => [(w1 D|w2 D)] => [(w d, 1)]
var unigSplit = unigrams.map(x => (x._1 + "\t" + x._2,1))	//[(w, D)] => [(w D, 1)]
val unigNonShared = unigSplit.subtractByKey(compSplit).map(x => (x._1.split("\t")(0), x._1.split("\t")(1))) //[(String, String)] = [(unig, D)]
val wordsAll = unigNonShared.union(compounds)
//---------------------------------------------------------------------------
// Remove Low Frequent Words
//---------------------------------------------------------------------------
val wc = wordsAll.map(x=> (x._1,1)).reduceByKey((a,b) => a+b).filter(x=> x._2 > 10)
val freqstopless = wc.join(wordsAll)
val documents = freqstopless.map(x => (((x._2)_2), x._1)).reduceByKey((a,b) => a + " " + b).filter(x => x._2.split(" ").size > 1).map(x => x._1 + "\t" + x._2)
documents.saveAsTextFile(folder + "RDC002.dc")


















/**
  * Created by kambiz on 9/9/16.
  */

