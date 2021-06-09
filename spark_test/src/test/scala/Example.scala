//
//import org.apache.kudu.client._
//import org.apache.kudu.spark.kudu._
//import org.apache.kudu.spark.kudu.KuduContext
//import org.apache.spark.sql.SparkSession
//
//import collection.JavaConverters._
//
//object Example {
//  val spark: SparkSession = SparkSession
//    .builder()
//    .master("local")
//    .getOrCreate()
//
//  import spark.implicits._
//
//  // Read a table from Kudu
//  val df = spark.read
//    .options(Map("kudu.master" -> "node1:7051", "kudu.table" -> "kudu_table"))
//    .format("kudu")
//    .load()
//
//  // Query using the Spark API...
//  df.select("key").filter("key >= 5").show()
//
//  // ...or register a temporary table and use SQL
//  df.createOrReplaceTempView("kudu_table")
//  val filteredDF = spark.sql("select key from kudu_table where key >= 5").show()
//
//  // Use KuduContext to create, delete, or write to Kudu tables
//  val kuduContext = new KuduContext("kudu.master:7051", spark.sparkContext)
//
//  // Create a new Kudu table from a DataFrame schema
//  // NB: No rows from the DataFrame are inserted into the table
//  kuduContext.createTable(
//    "test_table", df.schema, Seq("key"),
//    new CreateTableOptions()
//      .setNumReplicas(1)
//      .addHashPartitions(List("key").asJava, 3))
//
//  // Check for the existence of a Kudu table
//  kuduContext.tableExists("test_table")
//
//  // Insert data
//  kuduContext.insertRows(df, "test_table")
//
//  // Delete data
//  kuduContext.deleteRows(df, "test_table")
//
//  // Upsert data
//  kuduContext.upsertRows(df, "test_table")
//
//  // Update data
//  val updateDF = df.select($"key", ($"int_val" + 1).as("int_val"))
//  kuduContext.updateRows(updateDF, "test_table")
//
//  // Data can also be inserted into the Kudu table using the data source, though the methods on
//  // KuduContext are preferred
//  // NB: The default is to upsert rows; to perform standard inserts instead, set operation = insert
//  // in the options map
//  // NB: Only mode Append is supported
//  df.write
//    .options(Map("kudu.master" -> "kudu.master:7051", "kudu.table" -> "test_table"))
//    .mode("append")
//    .format("kudu").save
//
//  // Delete a Kudu table
//  kuduContext.deleteTable("test_table")
//}