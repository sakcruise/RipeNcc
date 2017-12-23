import org.apache.spark.{SparkConf, SparkContext}

object RipeNcc extends App {

  //val inputPath = args(0)
  val inputPath = "S:/Scala/data/ripencc/ripe_test.txt*"

  val conf = new SparkConf().
    setAppName("Spark RipeNcc").
    setMaster("local").
    set("spark.ui.port", "45678")

  val sc = new SparkContext(conf)

    val readFileRdd = sc.textFile(inputPath)

    val dataRdd_filter = readFileRdd.filter(r => r.split('|')(5).trim != "")
    val dataRdd = dataRdd_filter.map(r => (r.split('|')(6).split(" ").toList, r.split('|')(5)))
    val result = dataRdd.map(r => r._1.map(x => ((x,
                                                  getOrigOrTrans(r._1.last, x) + getPrefix(r._2)),
                                                  r._2)))
    val res_flat1 = result.flatMap(r => r)
//    val res_flat2 = res_flat1.map(r => { val p = r._1._1
//                                        if(p.contains("{")) p.replace("{" , "").split(",") else p. })
//    val res_group = res_flat.groupByKey
//    val res_sort = res_group.sortBy(r => r._1)
//    val res_final = res_sort.map(r => r._1._1 + '|' + r._1._2 + '|' + { if (r._2.size > 1) r._2.mkString(" ") else r._2 } )
//
//  res_final.take(50).foreach(println)
    res_flat1.take(50).foreach(println)

  def getOrigOrTrans(last: String, value: String): String = {
      if(last == value) "O" else "T"
  }

  def getPrefix(ipv: String): String = {
      if(ipv.contains(":")) "6" else "4"
  }
}
