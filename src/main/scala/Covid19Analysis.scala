import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Covid19Analysis {
  def main(args: Array[String]) {
    // Ορισμός ρυθμίσεων Spark
    val conf = new SparkConf().setAppName("Covid19Analysis").setMaster("local[*]")

    // Δημιουργία της συνεδρίας Spark
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Φόρτωση του CSV αρχείου σε ένα DataFrame
    val df = spark.read.option("header", "true").csv("src/input/covid.csv")

    // Ερώτημα 1: Πόσα νέα κρούσματα επιβεβαιώθηκαν καθημερινά κατά τον Δεκέμβριο του 2020 στην Ελλάδα;
    val december_2020_greece_cases_per_day = df.filter((substring(col("dateRep"), 4, 2) === "12") && (substring(col("dateRep"), 7, 4) === "2020") && (col("countriesAndTerritories") === "Greece"))
      .groupBy("dateRep")
      .agg(sum("cases").alias("daily_cases"))
      .orderBy("dateRep")
    // Εμφάνιση του αριθμού των κρουσμάτων ανά ημέρα στην Ελλάδα για τον Δεκέμβριο του 2020
    december_2020_greece_cases_per_day.show()


    // Ερώτημα 2: Ποιος είναι ο συνολικός αριθμός κρουσμάτων και θανάτων για κάθε ήπειρο;
    val total_cases_deaths_per_continent = df.groupBy("continentExp")
      .agg(sum("cases").alias("total_cases"), sum("deaths")
        .alias("total_deaths"))
      .orderBy("continentExp")

    // Μορφοποίηση των αριθμών για ευκολότερη ανάγνωση
    val formatted_tc = total_cases_deaths_per_continent.withColumn("total_cases", format_number(col("total_cases"), 0))
      .withColumn("total_deaths", format_number(col("total_deaths"), 0))
    formatted_tc.show()


    // Ερώτημα 3: Για τις χώρες της Ευρώπης, ποιος είναι ο μέσος όρος κρουσμάτων και θανάτων ανά χώρα;
    val european_countries_avg_cases_deaths = df.filter(col("continentExp") === "Europe")
      .groupBy("countriesAndTerritories")
      .agg(avg("cases")
        .alias("avg_cases"), avg("deaths")
        .alias("avg_deaths")).orderBy("countriesAndTerritories")
    // Εμφάνιση όλων των αποτελεσμάτων χωρίς περικοπή
    european_countries_avg_cases_deaths.show(european_countries_avg_cases_deaths.count.toInt, false)


    // Ερώτημα 4: Ποιες ήταν οι 10 χειρότερες ημερομηνίες σε σύνολο κρουσμάτων στην Ευρώπη;
    val worst_dates_europe = df.filter(col("continentExp") === "Europe")
      .groupBy("dateRep")
      .agg(sum("cases")
        .alias("total_cases"))
      .orderBy(desc("total_cases"))
      .limit(10)
    // Εμφάνιση των 10 ημερομηνιών με τα περισσότερα κρούσματα στην Ευρώπη
    worst_dates_europe.show()

    // Τερματισμός της συνεδρίας Spark
    spark.stop()
  }
}
