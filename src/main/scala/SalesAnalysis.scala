import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._

object SalesAnalysis {
  def main(args: Array[String]): Unit = {
    // Δημιουργία της SparkSession
    val spark = SparkSession.builder
      .appName("SalesAnalysis")
      .master("local[*]")
      .getOrCreate()


    // Διάβασμα του αρχείου CSV σε ένα DataFrame
    val salesDF = spark.read
      .option("header", "true")
      .csv("src/input/sales.csv")

    // Ορισμός της λειτουργίας για τον υπολογισμό του συνολικού κόστους ανά προϊόν ανά τιμολόγιο
    def calculateTotalCostPerInvoice(df: org.apache.spark.sql.DataFrame) = {
      df.withColumn("TotalCost", col("Quantity") * col("UnitPrice"))
        .groupBy("InvoiceNo", "CustomerID")
        .agg(sum("TotalCost").alias("TotalInvoiceCost"))
    }

    // Εφαρμογή της λειτουργίας στο DataFrame μας
    val salesWithTotalCost = calculateTotalCostPerInvoice(salesDF)

    // Τα 5 τιμολόγια με τη μεγαλύτερη αξία
    val topInvoices = salesWithTotalCost.orderBy(col("TotalInvoiceCost").desc).limit(5)
    topInvoices.show()

    // Τα 5 πιο δημοφιλή προϊόντα (περισσότερες πωλήσεις συνολικά)
    val popularProductsBySales = salesDF.groupBy("Description")
      .agg(sum("Quantity").alias("TotalSales"))
      .orderBy(col("TotalSales").desc)
      .limit(5)
    popularProductsBySales.show()

    // Τα 5 πιο δημοφιλή προϊόντα (πωλήθηκαν/συμπεριλαμβάνονται σε περισσότερα τιμολόγια)
    val popularProductsByInvoices = salesDF.groupBy("Description")
      .agg(count("InvoiceNo").alias("TotalInvoices"))
      .orderBy(col("TotalInvoices").desc)
      .limit(5)
    popularProductsByInvoices.show()

    // Ο μέσος όρος προϊόντων ανά τιμολόγιο και το μέσο κόστος τιμολογίου
    val averageProductsPerInvoice = salesDF.groupBy("InvoiceNo")
      .agg(count("StockCode").alias("TotalProductsPerInvoice"))
    val meanProductsPerInvoice = averageProductsPerInvoice.agg(avg("TotalProductsPerInvoice")).collect()(0)(0)

    val averageCostPerInvoice = salesWithTotalCost.agg(avg("TotalInvoiceCost")).collect()(0)(0)

    println(s"Ο μέσος όρος προϊόντων ανά τιμολόγιο είναι: $meanProductsPerInvoice")
    println(s"Το μέσο κόστος τιμολογίου είναι: $averageCostPerInvoice\n")

    // Οι 5 καλύτεροι πελάτες σε τζίρο
    val topCustomersByRevenue = salesWithTotalCost.groupBy("CustomerID")
      .agg(sum("TotalInvoiceCost").alias("TotalRevenue"))
      .orderBy(col("TotalRevenue").desc)
      .limit(5)
    topCustomersByRevenue.show()

    // Κλείσιμο της SparkSession
    spark.stop()
  }
}
