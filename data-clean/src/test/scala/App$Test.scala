import com.didi.config.TsvFilePath
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by twcn on 6/8/16.
  */
class App$Test extends FlatSpec with Matchers  {

  val conf = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
  val sc = new SparkContext(conf)


  it should "eq" in {

    val orderRDD = loadRDD(sc, TsvFilePath.orderFilePath)
    val trafficRDD = loadRDD(sc, TsvFilePath.trafficFilePath)
    val clusterRDD = loadRDD(sc, TsvFilePath.clusterFilePath)
    val poiRDD = loadRDD(sc, TsvFilePath.poiFilePath)
    val weatherRDD = loadRDD(sc, TsvFilePath.weatherFilePath)

    assert(orderRDD.count() == 8540614)
    assert(trafficRDD.count() == 193553)
    assert(clusterRDD.count() == 66)
    assert(poiRDD.count() == 66)
    assert(weatherRDD.count() == 4811)
  }

  private def loadRDD(sc: SparkContext,filePath: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath)
    rdd.map(_.split("\t"))
  }
}
