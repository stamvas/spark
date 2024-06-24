
import org.apache.spark.{SparkConf, SparkContext}

object WordCountRDD {
  def main(args: Array[String]): Unit = {
    // Ορισμός ρυθμίσεων Spark
    val conf = new SparkConf().setAppName("WordCountRDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Διαβάζουμε το αρχείο κειμένου
    val inputRDD = sc.textFile("src/input/Shakespeare.txt")
    //val inputRDD = sc.textFile("src/input/SherlockHolmes.txt")

    // Καθαρίζουμε τα δεδομένα και μετράμε τις λέξεις
    val wordCountRDD = inputRDD
      // Διαχωρίζουμε κάθε γραμμή σε λέξεις χρησιμοποιώντας το κενό ως διαχωριστικό
      .flatMap(line => line.split(" "))
      // Αφαιρούμε τα σημεία στίξης από κάθε λέξη και τις μετατρέπουμε σε πεζά γράμματα
      .map(word => word.replaceAll("[,.!?:;]", "").toLowerCase)
      // Φιλτράρουμε τις κενές λέξεις
      .filter(_.nonEmpty)
      // Δημιουργούμε ένα ζεύγος (λέξη, 1) για κάθε λέξη
      .map(word => (word, 1))


    // Υπολογισμός στατιστικών
    val wordCount = wordCountRDD.count()
    val totalWords = wordCountRDD.map(_._2).sum()
    // Υπολογίζουμε τον μέσο όρο του μήκους των λέξεων
    val avgWordLength = if (wordCount > 0) wordCountRDD.map(_._1.length).sum() / totalWords else 0.0
    // 'κόβουμε' τον μέσο όρο σε δύο δεκαδικά ψηφία
    val avgWordLengthFormatted = f"$avgWordLength%.2f"
    // Επιλέγουμε τις 5 πιο συχνά εμφανιζόμενες λέξεις
    val commonWords = wordCountRDD.filter(_._1.length > avgWordLength).reduceByKey(_ + _).takeOrdered(5)(Ordering[Int].reverse.on(_._2))

    // Εκτύπωση αποτελεσμάτων
    println(s"Πλήθος λέξεων: $wordCount")
    println(s"Μέσο μήκος λέξεων: $avgWordLengthFormatted")
    println("Οι 5 πιο συχνά εμφανιζόμενες λέξεις:")
    commonWords.foreach(println)

    // Κλείσιμο του SparkContext
    sc.stop()
  }
}




