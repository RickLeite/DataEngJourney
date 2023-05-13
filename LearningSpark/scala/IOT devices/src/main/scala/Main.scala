import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders

case class DeviceIoTData(
                          device_id: Long,
                          device_name: String,
                          ip: String,
                          cca2: String,
                          cca3: String,
                          cn: String,
                          latitude: Double,
                          longitude: Double,
                          scale: String,
                          temp: Long,
                          humidity: Long,
                          battery_level: Long,
                          c02_level: Long,
                          lcd: String,
                          timestamp: Long
                        )

object Main {
  def main(args: Array[String]): Unit = {
    println("Initializing SparkSession!")

    val spark = SparkSession
      .builder
      .appName("IOT Devices analysis")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(s"Spark version: ${spark.version}")

    val deviceIoTDataEncoder = Encoders.product[DeviceIoTData]
    val ds: Dataset[DeviceIoTData] = spark.read
      .json("/home/wedivv/Code/others/spark/LearningSpark/data/6-iot_devices.json")
      .as(deviceIoTDataEncoder)

    ds.show()

    val filterTempDS = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
    filterTempDS.show(5, false)



    import spark.implicits._

    ( ds
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      ).printSchema()


    (ds
      .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
      .where("temp > 25")
      ).show()

    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
      .where("temp > 25")


    System.out.println(" -------------- ")
    println(dsTemp2.show())
    System.out.println(dsTemp2.printSchema())


    spark.stop()
  }
}
