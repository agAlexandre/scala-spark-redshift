package ec2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.util.Properties

object sparkConf extends Serializable{
  def main(args:Array[String]){
    // Create SparkSession
    val sparkConf = SparkSession.builder
      .master("spark://localhost:7077") // TODO URL SPARK MASTER
      .appName("APP NAME") // TODO APP NAME SPARK
      .getOrCreate()
    // Create schema to Dataframe
    val schema = StructType (Array(StructField("Name Passenger", StringType, true), StructField("CPF", StringType, true), StructField ("Email Passenger", StringType
      , true), StructField("Phone Passenger", StringType, true)))
    // Create Dataframe
    val passengersDF = sparkConf.read.option("header",true)
      .schema(schema)
      .csv("/root/passengers.csv") // TODO PATH CSV FILE
      .cache()
      .show()
    // Redshift URL, Master User and Password
    import java.sql._

    var dbURL: String ="" //TODO REDSHIFT URL JDBC
    var MasterUsername: String ="awsuser"// TODO USER REDSHIFT, DEFAULT IS "awsuser"
    var MasterUserPassword: String = ""// TODO PASSWORD USER REDSHIFT
    //Redshift JDBC 4.1 driver: com.amazon.redshift.jdbc41.Driver
    Class.forName("com.amazon.redshift.jdbc.Driver")// TODO NEED REDSHIFT DRIVER DOWNLOAD ON AWS

    //Open a connection and define properties.
    val props = new Properties
    props.setProperty("user", MasterUsername)
    props.setProperty("password",MasterUserPassword)
    val connection= DriverManager.getConnection(dbURL, props)

    //Query

    val statement = connection.createStatement()
    val sql:String = "SELECT * FROM TABLE" // TODO QUERY SQL
    val result= statement.executeQuery(sql) // TODO RESULTS

    //Get the data from the result set
    while (result.next()){
      val column1:String = result.getString("column1") // TODO COLUMN NAME
      val column2:String = result.getString("column2")// TODO COLUMN NAME


      //Print values
      println("Column1: " + column1 +" | "+"Column2: "+ column2) //TODO PRINT LINE RESULTS

    }
    //Close connection
    result.close()
    statement.close()
    connection.close()
  }
}
