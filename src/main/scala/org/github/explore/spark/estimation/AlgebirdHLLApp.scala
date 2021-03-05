package org.github.explore.spark.estimation

import com.twitter.algebird.HyperLogLogMonoid


/**
  * we were able to call hll.create on an Int because we imported the implicit conversion int2Bytes that converts an Int into a Array[Byte].
  */
object AlgebirdHLLApp {
  def main(args: Array[String]): Unit = {
    println("This is the Spark test of the Algebird HyperLogLog application")

    val hll = new HyperLogLogMonoid(4)
    val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)

    // necessary to the following line
    import com.twitter.algebird.HyperLogLog.int2Bytes
    val seqHll = data.map { hll.create(_) }
    val sumHll = hll.sum(seqHll)
    val approxSizeOf = hll.sizeOf(sumHll)
    val actualSize = data.toSet.size
    val estimate = approxSizeOf.estimate

    println("Actual size: " + actualSize)
    println("Estimate size: " + estimate)
  }
}
