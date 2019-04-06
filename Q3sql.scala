// Tested on DATABRICKS
val rev = sc.textFile("/FileStore/tables/review.csv")
val bus = sc.textFile("/FileStore/tables/business.csv")

val rev_df = rev.map(Map1).toDF("B_id", "U_id", "Rating")
val bus_df = bus.map(Map2).toDF("B_id", "Categ")

rev_df.registerTempTable("review")
bus_df.registerTempTable("business")

val op = spark.sql("SELECT DISTINCT r.B_id, r.U_id, r.Rating " + 
                   "from review as r INNER JOIN business as b ON r.B_id = b.B_id " +
                   "where b.Categ like '%Colleges & Universities%' ").select("U_id", "Rating")

val res = op.rdd.map(_.toString().replace("[", "").replace("]", "")).coalesce(1, true)
res.saveAsTextFile("/FileStore/output")

def Map1(line: String) = {
    val data = line.split("::")
    (data(2), data(1), data(3))
}

def Map2(line: String) = {
    val data = line.split("::")
    (data(0), data(2))
}
