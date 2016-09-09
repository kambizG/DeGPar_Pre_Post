// Aggregate graphs to create the final graph
val graph1 = sc.textFile("/home/kambiz/data/experiments_20%/graphs/graph_1000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph2 = sc.textFile("/home/kambiz/data/experiments_20%/graphs/graph_2000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph3 = sc.textFile("/home/kambiz/data/experiments_20%/graphs/graph_3000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph4 = sc.textFile("/home/kambiz/data/experiments_20%/graphs/graph_4000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph5 = sc.textFile("/home/kambiz/data/experiments_20%/graphs/graph_5000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))

val gg_2m = graph1.union(graph2).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_2m.saveAsTextFile("/home/kambiz/data/experiments_20%/exp_05/graph_aggregate")

val gg_3m = graph1.union(graph2).union(graph3).union(graph5).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_3m.saveAsTextFile("/home/kambiz/data/experiments_20%/exp_06/graph_aggregate")

val gg_4m = graph1.union(graph2).union(graph3).union(graph4).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_4m.saveAsTextFile("/home/kambiz/data/experiments_20%/exp_07/graph_aggregate")

val gg_5m = graph1.union(graph2).union(graph3).union(graph4).union(graph5).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_5m.saveAsTextFile("/home/kambiz/data/experiments_20%/exp_08/graph_aggregate")


for i in {500000..5000000..500000}; do cat RDG00_$i.gr/part-00* >> graph_$i.txt; done

val graph1 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_500000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph2 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_1000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph3 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_1500000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph4 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_2000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph5 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_2500000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph6 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_3000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph7 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_3500000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph8 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_4000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph9 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_4500000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))
val graph10 = sc.textFile("/home/kambiz/data/experiments_20%_100/graph_cosine/graph_5000000.txt").map(x => (x.split("\t")(0), x.split("\t")(1).toDouble))


val gg_1m = graph1.union(graph2).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_1m.saveAsTextFile("/home/kambiz/data/experiments_20%_100/exp_02/graph_aggregate")

val gg_3m = graph1.union(graph2).union(graph3).union(graph4).union(graph5).union(graph6).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_3m.saveAsTextFile("/home/kambiz/data/experiments_20%_100/exp_03/graph_aggregate")

val gg_5m = graph1.union(graph2).union(graph3).union(graph4).union(graph5).union(graph6).union(graph7).union(graph8).union(graph9).union(graph10).reduceByKey((a,b) => a+b).map(x => (x._1 + "\t" + x._2))
gg_5m.saveAsTextFile("/home/kambiz/data/experiments_20%_100/exp_04/graph_aggregate")


// Topic extraction for 10 experiments, repeated 10 times per experiment
java -jar DeGPar.jar /home/kambiz/data/experiments_20%_100/exp_00/  >  /home/kambiz/DeGPar/reports/report_00.log 2>&1 &
java -jar DeGPar.jar /home/kambiz/data/experiments_20%_100/exp_01/  >  /home/kambiz/DeGPar/reports/report_01.log 2>&1 &
java -jar DeGPar.jar /home/kambiz/data/experiments_20%_100/exp_02/  >  /home/kambiz/DeGPar/reports/report_02.log 2>&1 &
java -jar DeGPar.jar /home/kambiz/data/experiments_20%_100/exp_03/  >  /home/kambiz/DeGPar/reports/report_03.log 2>&1 &
java -jar DeGPar.jar /home/kambiz/data/experiments_20%_100/exp_04/  >  /home/kambiz/DeGPar/reports/report_04.log 2>&1 &


grep -i 'Execution terminated' exp_04/14701*/report.log | awk '{print $11}' | cut -d's' -f1

echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_0/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_0/report.log 2>&1 &" >> script_0_1.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_1/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_1/report.log 2>&1 &" >> script_0_1.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_2/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_2/report.log 2>&1 &" >> script_2_3.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_3/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_3/report.log 2>&1 &" >> script_3_4.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_4/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_4/report.log 2>&1 &" >> script_3_4.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_5/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_5/report.log 2>&1 &" >> script_5_6.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_6/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_6/report.log 2>&1 &" >> script_5_6.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_7/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_7/report.log 2>&1 &" >> script_7_8.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_8/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_8/report.log 2>&1 &" >> script_7_8.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_03/trial_9/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/trial_9/report.log 2>&1 &" >> script_9.sh

echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_0/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_0/report.log 2>&1 &" >> script_0_1.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_1/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_1/report.log 2>&1 &" >> script_0_1.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_2/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_2/report.log 2>&1 &" >> script_2_3.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_3/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_3/report.log 2>&1 &" >> script_2_3.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_4/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_4/report.log 2>&1 &" >> script_4_5.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_5/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_5/report.log 2>&1 &" >> script_4_5.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_6/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_6/report.log 2>&1 &" >> script_6_7.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_7/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_7/report.log 2>&1 &" >> script_6_7.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_8/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_8/report.log 2>&1 &" >> script_8_9.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 70g -i /home/kambiz/data/experiments/exp_04/trial_9/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/trial_9/report.log 2>&1 &" >> script_8_9.sh



for i in {6000000..7000000..1000000}; do cat RDG00_$i.gr/part-00* >> gr/graph_$i.txt; done
for i in {100000..1000000..100000}; do cat docTopic_$i/part-00* >> communities/community_$i.txt; done
for i in {0..9}; do cp exp_0$i/graph_* exp_0$i/00_graph.txt ;done
for i in {0..9}; do rm exp_0$i/graph_* ;done

for i in {0..9}; do scp exp_0$i/00_graph.txt kambiz@n174-p92.kthopen.kth.se:/Users/kambiz/Documents/workspace/Gavagai/tw_lo_har/exp_0$i/00_graph.txt ;done

for j in {1..9}; do for i in {0..9}; do mkdir exp_0$j/trial_$i; done; done


for each experiment(exp_X):{
// Run Algorithm 10 times >> trial{0..9}
java -jar DeGPar.jar /home/kambiz/data/experiments/exp_X/  >  /home/kambiz/DeGPar/reports/report_00.log 2>&1 &
04 - Extract_communities (DOCTOPIC)
for i in {0..9}; do cat trial_$i/docTopic_100000/part-000* >> trial_$i/docTopic.txt ; done
top_10 Words:
	- 05 - Evaluation
	- grep -i 'Coherency' log/report.log  | awk -F',' '{print $3}'| cut -d')' -f1
}

for i in {0..9}; do cat trial_$i/docTopic_*/part-00* >> trial_$i/docTopic.txt; done
for i in {0..9}; do rm -r trial_$i/docTopic_* ;done

echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_03/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_03/report.log 2>&1 &" >> script.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_04/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_04/report.log 2>&1 &" >> script.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_05/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_05/report.log 2>&1 &" >> exp_05/script.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_06/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_06/report.log 2>&1 &" >> exp_06/script.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_07/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_07/report.log 2>&1 &" >> exp_07/script.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_08/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_08/report.log 2>&1 &" >> exp_08/script.sh
echo "/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 140g -i /home/kambiz/data/experiments/exp_09/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_09/report.log 2>&1 &" >> exp_09/script.sh

/home/kambiz/spark-1.6.2/bin/spark-shell --driver-memory 120g -i /home/kambiz/data/experiments/exp_06/04_extract_communities.scala >  /home/kambiz/data/experiments/exp_06/report.log 2>&1 &


File Size
ls -lh

directory size
du -hs 

kill -9 PID
