// Tested on DATABRICKS 
val input = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

    // output from 1st task is <friend-pair> \t <their common friends' ids>
    val op1 = input.flatMap(Map1)
      .reduceByKey(Reduce1)
      .filter(!_._2.equals(0)).filter(!_._2.isEmpty)
      .sortByKey()

    // output from 2nd (map-reduce) task is <friend-pair>\t<num of common friends of friend-pair> in decreasing order of num of comm friends
    val op2 = op1.map(valu => (valu._1, valu._2.size))
      .map(elem => elem.swap)
      .sortByKey(false, 1)
      .map(elem => elem.swap)

    // pick the top-ten entries
    val top_ten = op2.zipWithIndex()
      .filter { case (_, index) => index < 10 }.map { case (value, index) => (value._1, value._2) }

    val userdata_file = sc.textFile("/FileStore/tables/userdata.txt")
    val u_data = userdata_file.flatMap(Map2)

    val top_res = top_ten.flatMap {
      value: (String, Int) =>
        val keys = value._1.split(",").toVector
        keys.map((k: String) => (k, value._1 + ":" + value._2.toString))
    }

    val res = top_res.join(u_data)

    val map3 = res.map {
      value =>
        val k = value._1
        val key = value._2._1
        val vals = value._2._2(0) + "\t" + value._2._2(1) + "\t" + value._2._2(2)
        (key, vals)
    }

    val Output = map3.reduceByKey(_ + "\t" + _).map(value => value._1.split(":")(1) + "\t" + value._2)

    Output.saveAsTextFile("/FileStore/output")

  def Reduce1(Ist: Set[String], IInd: Set[String]) = {
      Ist.toSet intersect IInd.toSet
  }

  def Map1(line: String) = {
    val ip = line.split("\\t+")
    val person = ip(0)
    val user_friends = if (ip.length > 1) ip(1) else "null"
    val nfriends = user_friends.split(",")
    val friends = for (i <- 0 to nfriends.length - 1) yield nfriends(i)
    val pairs = nfriends.map(friend => {
      if (person < friend) person + "," + friend else friend + "," + person
    })
    pairs.map(pair => (pair, friends.toSet))
  }

  def Map2(line: String) = {
    val ip = line.split(",")
    val data = Array(for (i <- 0 until ip.length - 1) yield ip(i))
    data.map(elem => (elem(0), elem.slice(1, elem.length - 1)))
}
