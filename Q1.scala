// Tested on DATAbricks
val soc = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val res = soc.flatMap(Map)
 .reduceByKey(intersection)
 .filter(!_._2.equals("null")).filter(!_._2.isEmpty)
 .sortByKey()
val op = res.map(value => value._1 + "\t" + value._2.size)
//res.foreach(println)
op.saveAsTextFile("/FileStore/output")

def intersection(first: Set[String], second: Set[String]) = {
    first.toSet intersect second.toSet
}

def Map(line: String) = {
    val ip = line.split("\\t+")
    val user = ip(0)
    val user_friends = if (ip.length > 1) ip(1) else "null"
    val nfriends = user_friends.split(",")
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    val pairs = nfriends.map(friend => {
      if (user < friend) user + "," + friend else friend + "," + user
    })
    pairs.map(pair => (pair, friends.toSet))
}
