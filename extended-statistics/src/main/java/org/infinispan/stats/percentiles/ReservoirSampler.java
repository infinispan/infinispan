package org.infinispan.stats.percentiles;

import org.infinispan.stats.exception.PercentileOutOfBounds;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps the sample for percentile calculations.
 *
 * @author Roberto Palmieri
 * @author Diego Didona
 * @author Pedro Ruivo
 * @since 5.3
 */
public class ReservoirSampler {

   private static final int DEFAULT_NUM_SPOTS = 100;
   private final AtomicInteger index;
   private final Random rand;
   private double[] reservoir;
   private int numSpot;

   public ReservoirSampler() {
      this(DEFAULT_NUM_SPOTS);
   }

   public ReservoirSampler(int numSpots) {
      this.numSpot = numSpots;
      this.reservoir = createArray();
      this.index = new AtomicInteger(0);
      rand = new Random(System.nanoTime());

   }

   /**
    * inserts a sample
    *
    * @param sample
    */
   public final void insertSample(double sample) {
      int i = index.getAndIncrement();
      if (i < numSpot)
         reservoir[i] = sample;
      else {
         int rand_generated = rand.nextInt(i + 2);//should be nextInt(index+1) but nextInt is exclusive
         if (rand_generated < numSpot) {
            reservoir[rand_generated] = sample;
         }
      }
   }

   /**
    * @param k the percentage of observations. Should be a value between 0 and 100 exclusively.
    * @return the percentile value for the k% observations.
    * @throws PercentileOutOfBounds if k is not between 0 and 100 exclusively.
    */
   public final double getKPercentile(int k) throws PercentileOutOfBounds {
      if (k < 0 || k > 100) {
         throw new PercentileOutOfBounds(k);
      }
      double[] copy = createArray();
      System.arraycopy(this.reservoir, 0, copy, 0, numSpot);
      Arrays.sort(copy);
      return copy[this.getIndex(k)];
   }

   /**
    * resets the samples.
    */
   public final void reset() {
      this.index.set(0);
      this.reservoir = createArray();
   }

   private int getIndex(int k) {
      //I solve the proportion k:100=x:NUM_SAMPLE
      //Every percentage is covered by NUM_SAMPLE / 100 buckets; I consider here only the first as representative
      //of a percentage
      return numSpot * (k - 1) / 100;
   }

   private double[] createArray() {
      return new double[numSpot];
   }
}


