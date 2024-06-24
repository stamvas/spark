import org.apache.spark.{SparkConf, SparkContext}

object ChessRDD {
  def main(args: Array[String]): Unit = {
    // Ορισμός ρυθμίσεων Spark
    val conf = new SparkConf().setAppName("ChessRDD").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Διαβάζουμε το αρχείο κειμένου
    val inputRDD = sc.textFile("src/input/games.csv")

    // Υπολογισμός ζευγαριών παικτών με λευκά πιόνια
    val whitePlayerPairs  = inputRDD.map(line => {
      val cols = line.split(",")
      (cols(8), cols(10)) // Ο παίκτης με λευκά πιόνια βρίσκεται στην 8η στήλη και ο παίκτης με μαύρα στην 10η
    })
    // Υπολογισμός ζευγαριών παικτών με μαύρα πιόνια
    val blackPlayerPairs = inputRDD.map(line => {
      val cols = line.split(",")
      (cols(10), cols(8))
    })
    // Συνδυασμός των δύο RDD για την εύρεση όλων των ζευγαριών παικτών
    val allPlayerPairs = whitePlayerPairs.union(blackPlayerPairs)
    // Υπολογισμός του αριθμού των παιχνιδιών για κάθε ζευγάρι παικτών
    val playerPairsCount = allPlayerPairs.map(pair => (pair, 1)).reduceByKey(_ + _)
    // Επιλογή των ζευγαριών παικτών που έχουν παίξει πάνω από 5 φορές και εκτύπωση αποτελεσμάτων
    val frequentPairs = playerPairsCount.filter(pairCount => pairCount._2 > 5).sortBy(-_._2)
    println("Ζευγάρια παικτών που έχουν παίξει πάνω από 5 φορές:")
    frequentPairs.collect().foreach(pair => {
      // Εκτύπωση μόνο μίας εγγραφής ανά ζεύγος παικτών
      if (pair._1._1 < pair._1._2) {
        println("(" + pair._1._1 + "_" + pair._1._2 + "," + pair._2 + ")")
      }
    })

    // Υπολογισμός των 5 πιο κοινών κινήσεων και εκτύπωση αποτελεσμάτων
    val moves = inputRDD.map(line => line.split(",")(12))
    val individualMoves = moves.flatMap(_.split(" "))
    val moveCounts = individualMoves.map(move => (move, 1)).reduceByKey(_ + _)
    val top5Moves = moveCounts.takeOrdered(5)(Ordering[Int].reverse.on(_._2))
    println("\nΟι 5 πιο κοινές κινήσεις:")
    top5Moves.foreach(println)

    // Τερματισμός του SparkContext
    sc.stop()
  }
}
