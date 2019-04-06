val rev = sc.textFile("/FileStore/tables/review.csv")
val bus = sc.textFile("/FileStore/tables/business.csv")

val rev_df = rev.map(Map1).toDF("B_id", "Rating")
val bus_df = bus.map(Map2).toDF("B_id", "Addr", "Categ")

rev_df.registerTempTable("review")
bus_df.registerTempTable("business")

val op = spark.sql("SELECT DISTINCT r.B_id, b.Addr, b.Categ, sum(r.Rating)/count(r.Rating) as Avg_Rating " + 
                   "from review as r INNER JOIN business as b ON r.B_id = b.B_id " +
                   "where b.Addr like '%NY%' " +
                   "GROUP BY r.B_id, b.Addr, b.Categ " +
                   "ORDER BY Avg_Rating desc LIMIT 10")

val res = op.rdd.map(_.toString().replace("[", "").replace("]", "")).coalesce(1, true)
res.saveAsTextFile("/FileStore/output")

def Map1(line: String) = {
    val data = line.split("::")
    (data(2), data(3))
}

def Map2(line: String) = {
    val data = line.split("::")
    (data(0), data(1), data(2))
}
