import com.didi.config.{CsvFilePath, TsvFilePath}
import com.didi.merge.Merge
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[1]").setAppName("DIDI-TECH")
    val sc = new SparkContext(conf)

    val orderRDD = loadRDD(sc, TsvFilePath.orderFilePath)
    val trafficRDD = loadRDD(sc, TsvFilePath.trafficFilePath)
    val clusterRDD = loadRDD(sc, TsvFilePath.clusterFilePath)
    val poiRDD = loadRDD(sc, TsvFilePath.poiFilePath)
    val weatherRDD = loadRDD(sc, TsvFilePath.weatherFilePath)


    val response = Merge.mergeRDD(orderRDD, trafficRDD, clusterRDD, poiRDD, weatherRDD)
    saveRDD2CSV(response)
  }


  private def saveRDD2CSV(rdd: RDD[Array[String]]): Unit = {
    rdd.map(row => row.mkString(",")).saveAsTextFile(CsvFilePath.saveMergeFilePath)
  }


  private def loadRDD(sc: SparkContext,filePath: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath)
    rdd.map(_.split("\t"))
  }

}
