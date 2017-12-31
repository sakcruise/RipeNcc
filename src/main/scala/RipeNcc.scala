import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config._
import org.apache.hadoop.fs._

object RipeNcc extends App {
  System.setProperty("hadoop.home.dir", """C:\hadoop""")
  val props = ConfigFactory.load()
    val conf = new SparkConf().
    setAppName("Spark RipeNcc").
    setMaster(props.getConfig(args(0)).getString("executionMode")).
    set("spark.ui.port", "45678")

  val sc = new SparkContext(conf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  val inputPath = args(1)
  val outputPath = args(2)
  val inPath = new Path(inputPath)
  val outPath = new Path(outputPath)
  if(!fs.exists(inPath)) {
      println("Input Path does not exists")
    }
  else {
    if(fs.exists(outPath)) {
      fs.delete(outPath, true)
    }
try {
  val readFileRdd = sc.textFile(inputPath)

  val dataRdd_filter = readFileRdd.filter(r => r.split('|')(5).trim != "")     /* filters the lines with prefixes */
  val dataRdd = dataRdd_filter.map(r => (r.split('|')(6).split(" ").toList, r.split('|')(5)))  /* extract only asn numbers and prefix */
  val result = dataRdd.map(r => r._1.map(x => ((x,
                        getOrigOrTrans(r._1.last, x) + getPrefix(r._2)),
                        r._2)))                                       /* extract key as (asn number , (O/T + 4/6)) and (prefix)  */
  val res_flat1 = result.flatMap(r => r)   /* flatten */
  val res_flat2 = res_flat1.filter(r => !(r._1._1.contains("{") || r._1._1.contains("}")))   /* extract the non set asn numbers from asn path*/
  val res_flat3 = res_flat1.filter(r => r._1._1.contains("{") || r._1._1.contains("}")).
                            map(r => {
                              val p = r._1._1
                              p.replace("{", "").replace("}", "").split(",").toList.map(x => ((x, r._1._2), (r._2)))
                            })                                      /* handling of sets - extract the asn numbers set from asn path and convert it as list */
  val res_flat4 = res_flat3.flatMap(r => r)   /* flatten the list */
  val res_union = res_flat2.union(res_flat4)  /* join both the lists by union */
  val res_group = res_union.groupByKey        /* group by key */
  val res_sort = res_group.sortBy(r => r._1._1.toInt)     /* sort by only asn number */
  val res_final = res_sort.map(r => r._1._1 + '|' + r._1._2 + '|' + {
    if (r._2.size > 1) r._2.mkString(" ") else r._2.toString
  })

  //  res_final.take(50).foreach(println)
  res_final.saveAsTextFile(outputPath)    /* save to output file */
}
    catch {
      case e => println(e)
    }


  }


  def getOrigOrTrans(last: String, value: String): String = {
      if(last == value) "O" else "T"
  }

  def getPrefix(ipv: String): String = {
      if(ipv.contains(":")) "6" else "4"
  }
}
