package ec2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object sparkConf extends Serializable{
  def main(args: Array[String]){
    // Create SparkSession
    val sparkConf = SparkSession.builder
      .master("spark://DESKTOP-0V26EF4.localdomain:7077")
      .appName("CSV PASSENGERS")
      .getOrCreate()
    // Create schema to Dataframe
    val schema = StructType (Array(StructField("Name Passenger", StringType, true), StructField("CPF", StringType, true), StructField ("Email Passenger", StringType
      , true), StructField("Phone Passenger", StringType, true)))
    // Create Dataframe
    val passengersDF = sparkConf.read.option("header",true)
      .schema(schema)
      .csv("/root/passengers.csv")
      .cache()
      .show()
  }
}
