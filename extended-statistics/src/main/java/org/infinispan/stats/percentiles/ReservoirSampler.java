package org.infinispan.stats.percentiles;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the sample for percentile calculations.
 * <p/>
 * Please check <a href="http://en.wikipedia.org/wiki/Reservoir_sampling for more details">this</a> for more details
 *
 * @author Roberto Palmieri
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ReservoirSampler {

   private static final int DEFAULT_NUM_SPOTS = 100;
   private final AtomicInteger index;
   private final Random random;
   private double[] reservoir;

   public ReservoirSampler() {
      this(DEFAULT_NUM_SPOTS);
   }

   public ReservoirSampler(int numSpots) {
      this.reservoir = createArray(numSpots);
      this.index = new AtomicInteger(0);
      random = new Random(System.nanoTime());

   }

   public synchronized final void insertSample(double sample) {
      int i = index.getAndIncrement();
      if (i < reservoir.length)
         reservoir[i] = sample;
      else {
         int randGenerated = random.nextInt(i + 2);//should be nextInt(index+1) but nextInt is exclusive
         if (randGenerated < reservoir.length) {
            reservoir[randGenerated] = sample;
         }
      }
   }

   /**
    * @param k the percentage of observations. Should be a value between 0 and 100 exclusively.
    * @return the percentile value for the k% observations.
    * @throws IllegalArgumentException if k is not between 0 and 100 exclusively.
    */
   public synchronized final double getKPercentile(int k) throws IllegalArgumentException {
      if (k < 0 || k > 100) {
         throw new IllegalArgumentException(k + " should be between 0 and 100 exclusive");
      }
      double[] copy = createArray(reservoir.length);
      System.arraycopy(this.reservoir, 0, copy, 0, reservoir.length);
      Arrays.sort(copy);
      return copy[this.getIndex(k)];
   }

   public synchronized final void reset() {
      this.index.set(0);
      this.reservoir = createArray(reservoir.length);
   }

   private int getIndex(int k) {
      //I solve the proportion k:100=x:NUM_SAMPLE
      //Every percentage is covered by NUM_SAMPLE / 100 buckets; I consider here only the first as representative
      //of a percentage
      return reservoir.length * (k - 1) / 100;
   }

   private double[] createArray(int size) {
      return new double[size];
   }
}


