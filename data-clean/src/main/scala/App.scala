import com.didi.config.{CsvFilePath, TsvFilePath}
import com.didi.merge.Merge
import com.didi.models._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object App {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[1]").setAppName("DIDI-TECH")
    val sc = new SparkContext(conf)


    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val orderRDD = loadRDD(sc, TsvFilePath.orderFilePath)
    val trafficRDD = loadRDD(sc, TsvFilePath.trafficFilePath)
    val clusterRDD = loadRDD(sc, TsvFilePath.clusterFilePath)
    val poiRDD = loadRDD(sc, TsvFilePath.poiFilePath)
    val weatherRDD = loadRDD(sc, TsvFilePath.weatherFilePath)


    val orderDataFrame = orderRDD.map(f => Order(f(0), f(1), f(2), f(3), f(4), f(5).trim.toDouble, f(6))).toDF()
    orderDataFrame.registerTempTable("order")

    val trafficDataFrame = trafficRDD.map(f => Traffic(f(0), f(1), f(2))).toDF()
    trafficDataFrame.registerTempTable("traffic")


    val clusterDataFrame = clusterRDD.map(f => Cluster(f(0), f(1))).toDF()
    clusterDataFrame.registerTempTable("cluster")

    val poiDataFrame = poiRDD.map(f => Poi(f(0), f(1))).toDF()
    poiDataFrame.registerTempTable("poi")

    val weatherDataFrame = weatherRDD.map(f => Weather(f(0), f(1).trim.toInt, f(2).trim.toDouble, f(3).trim.toDouble)).toDF()
    weatherDataFrame.registerTempTable("weather")



    val trafficWithRegion = clusterDataFrame.join(trafficDataFrame, clusterDataFrame("districtHash") === trafficDataFrame("districtHash"), "left")
    trafficWithRegion.show()
    val s = orderDataFrame.join(weatherDataFrame, orderDataFrame("time") === weatherDataFrame("time"), "left")
    s.show()
  }



  private def join(dataFrames: Array[DataFrame]): DataFrame = {
    ???
  }

  private def saveRDD2CSV(rdd: RDD[Array[String]]): Unit = {
    rdd.map(row => row.mkString(",")).saveAsTextFile(CsvFilePath.saveMergeFilePath)
  }


  private def loadRDD(sc: SparkContext,filePath: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath)
    rdd.map(_.split("\t"))
  }

}
