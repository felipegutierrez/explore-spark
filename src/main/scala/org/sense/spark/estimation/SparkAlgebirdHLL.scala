package org.sense.spark.estimation

import com.twitter.algebird.{HLL, HyperLogLogMonoid}

object SparkAlgebirdHLL {

  // Precision for HLL algorithm, computed as 1.04/sqrt(2^{bits})
  private val HLL_PRECISION = 18

  // Global HLL instance
  private val HYPER_LOG_LOG_MONOID = new HyperLogLogMonoid(HLL_PRECISION)


}
