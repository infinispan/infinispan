package org.infinispan.server.resp.commands.tdigest;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A T-Digest implementation compatible with Redis TDIGEST commands.
 * <p>
 * T-Digest is a probabilistic data structure for accurate estimation of
 * quantiles and percentiles from streaming data. It maintains a set of
 * centroids that summarize the distribution.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_T_DIGEST)
public final class TDigest {

   public static final int DEFAULT_COMPRESSION = 100;

   private final int compression;
   private final List<Centroid> centroids;
   private long totalWeight;
   private double min;
   private double max;

   public TDigest(int compression) {
      this.compression = compression;
      this.centroids = new ArrayList<>();
      this.totalWeight = 0;
      this.min = Double.POSITIVE_INFINITY;
      this.max = Double.NEGATIVE_INFINITY;
   }

   @ProtoFactory
   TDigest(int compression, List<Centroid> centroids, long totalWeight, double min, double max) {
      this.compression = compression;
      this.centroids = centroids != null ? new ArrayList<>(centroids) : new ArrayList<>();
      this.totalWeight = totalWeight;
      this.min = min;
      this.max = max;
   }

   @ProtoField(number = 1, defaultValue = "100")
   public int getCompression() {
      return compression;
   }

   @ProtoField(number = 2)
   public List<Centroid> getCentroids() {
      return centroids;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public long getTotalWeight() {
      return totalWeight;
   }

   @ProtoField(number = 4, defaultValue = "Infinity")
   public double getMin() {
      return min;
   }

   @ProtoField(number = 5, defaultValue = "-Infinity")
   public double getMax() {
      return max;
   }

   /**
    * Adds a value to the t-digest.
    */
   public void add(double value) {
      add(value, 1);
   }

   /**
    * Adds a value with a weight to the t-digest.
    */
   public void add(double value, long weight) {
      if (Double.isNaN(value)) {
         return;
      }

      min = Math.min(min, value);
      max = Math.max(max, value);

      // Find closest centroid
      int closest = -1;
      double minDistance = Double.MAX_VALUE;

      for (int i = 0; i < centroids.size(); i++) {
         double dist = Math.abs(centroids.get(i).mean - value);
         if (dist < minDistance) {
            minDistance = dist;
            closest = i;
         }
      }

      if (closest == -1 || !canMerge(closest, weight)) {
         // Create new centroid
         centroids.add(new Centroid(value, weight));
         totalWeight += weight;
         compress();
      } else {
         // Merge with existing centroid
         Centroid c = centroids.get(closest);
         double newWeight = c.weight + weight;
         double newMean = (c.mean * c.weight + value * weight) / newWeight;
         centroids.set(closest, new Centroid(newMean, (long) newWeight));
         totalWeight += weight;
      }
   }

   private boolean canMerge(int index, long additionalWeight) {
      if (totalWeight == 0) return true;

      Centroid c = centroids.get(index);
      double quantile = cumulativeWeight(index) / (double) totalWeight;
      double limit = 4 * compression * quantile * (1 - quantile);
      return c.weight + additionalWeight <= limit;
   }

   private double cumulativeWeight(int index) {
      double sum = 0;
      for (int i = 0; i < index; i++) {
         sum += centroids.get(i).weight;
      }
      return sum + centroids.get(index).weight / 2.0;
   }

   private void compress() {
      if (centroids.size() <= compression) {
         return;
      }

      // Sort centroids by mean
      centroids.sort(Comparator.comparingDouble(c -> c.mean));

      // Merge adjacent centroids
      List<Centroid> compressed = new ArrayList<>();
      Centroid current = centroids.get(0);

      for (int i = 1; i < centroids.size(); i++) {
         Centroid next = centroids.get(i);
         double newWeight = current.weight + next.weight;
         double quantile = (cumulativeWeightInList(compressed, current) + newWeight / 2.0) / totalWeight;
         double limit = 4 * compression * quantile * (1 - quantile);

         if (newWeight <= limit) {
            // Merge
            double newMean = (current.mean * current.weight + next.mean * next.weight) / newWeight;
            current = new Centroid(newMean, (long) newWeight);
         } else {
            compressed.add(current);
            current = next;
         }
      }
      compressed.add(current);

      centroids.clear();
      centroids.addAll(compressed);
   }

   private double cumulativeWeightInList(List<Centroid> list, Centroid current) {
      double sum = 0;
      for (Centroid c : list) {
         sum += c.weight;
      }
      return sum + current.weight / 2.0;
   }

   /**
    * Resets the t-digest, removing all data.
    */
   public void reset() {
      centroids.clear();
      totalWeight = 0;
      min = Double.POSITIVE_INFINITY;
      max = Double.NEGATIVE_INFINITY;
   }

   /**
    * Returns the minimum value.
    */
   public double min() {
      return totalWeight == 0 ? Double.NaN : min;
   }

   /**
    * Returns the maximum value.
    */
   public double max() {
      return totalWeight == 0 ? Double.NaN : max;
   }

   /**
    * Returns the quantile value for a given fraction (0 to 1).
    */
   public double quantile(double fraction) {
      if (totalWeight == 0 || centroids.isEmpty()) {
         return Double.NaN;
      }
      if (fraction <= 0) {
         return min;
      }
      if (fraction >= 1) {
         return max;
      }

      // Sort centroids
      List<Centroid> sorted = new ArrayList<>(centroids);
      sorted.sort(Comparator.comparingDouble(c -> c.mean));

      double targetWeight = fraction * totalWeight;
      double cumulativeWeight = 0;

      for (int i = 0; i < sorted.size(); i++) {
         Centroid c = sorted.get(i);
         double centroidStart = cumulativeWeight;
         double centroidEnd = cumulativeWeight + c.weight;

         if (targetWeight <= centroidEnd) {
            // Linear interpolation within centroid range
            double leftValue, rightValue;
            if (i == 0) {
               leftValue = min;
            } else {
               leftValue = (sorted.get(i - 1).mean + c.mean) / 2;
            }
            if (i == sorted.size() - 1) {
               rightValue = max;
            } else {
               rightValue = (c.mean + sorted.get(i + 1).mean) / 2;
            }

            double localFraction = (targetWeight - centroidStart) / c.weight;
            return leftValue + (rightValue - leftValue) * localFraction;
         }
         cumulativeWeight = centroidEnd;
      }

      return max;
   }

   private double interpolate(double a, double b, double fraction) {
      return a + (b - a) * fraction;
   }

   /**
    * Returns the CDF value (fraction of values less than or equal to x).
    */
   public double cdf(double x) {
      if (totalWeight == 0 || centroids.isEmpty()) {
         return Double.NaN;
      }
      if (x <= min) {
         return 0;
      }
      if (x >= max) {
         return 1;
      }

      List<Centroid> sorted = new ArrayList<>(centroids);
      sorted.sort(Comparator.comparingDouble(c -> c.mean));

      // Calculate cumulative weight up to where x falls
      double weightSoFar = 0;

      for (int i = 0; i < sorted.size(); i++) {
         Centroid c = sorted.get(i);
         double leftBound, rightBound;

         // Determine left boundary
         if (i == 0) {
            leftBound = min;
         } else {
            leftBound = (sorted.get(i - 1).mean + c.mean) / 2;
         }

         // Determine right boundary
         if (i == sorted.size() - 1) {
            rightBound = max;
         } else {
            rightBound = (c.mean + sorted.get(i + 1).mean) / 2;
         }

         if (x < leftBound) {
            // x is before this centroid's range
            return weightSoFar / totalWeight;
         }

         if (x <= rightBound) {
            // x is within this centroid's range, interpolate
            double fraction = (x - leftBound) / (rightBound - leftBound);
            return (weightSoFar + fraction * c.weight) / totalWeight;
         }

         // x is past this centroid's range
         weightSoFar += c.weight;
      }

      return weightSoFar / totalWeight;
   }

   /**
    * Returns the rank (number of values less than or equal to x).
    */
   public long rank(double x) {
      double cdfValue = cdf(x);
      if (Double.isNaN(cdfValue)) {
         return -1;
      }
      return Math.round(cdfValue * totalWeight);
   }

   /**
    * Returns the reverse rank (number of values greater than x).
    */
   public long revRank(double x) {
      double cdfValue = cdf(x);
      if (Double.isNaN(cdfValue)) {
         return -1;
      }
      return Math.round((1 - cdfValue) * totalWeight);
   }

   /**
    * Returns the value at the given rank.
    */
   public double byRank(long rank) {
      if (totalWeight == 0) {
         return Double.NaN;
      }
      if (rank <= 0) {
         return min;
      }
      if (rank >= totalWeight) {
         return max;
      }
      return quantile((double) rank / totalWeight);
   }

   /**
    * Returns the value at the given reverse rank.
    */
   public double byRevRank(long revRank) {
      if (totalWeight == 0) {
         return Double.NaN;
      }
      if (revRank <= 0) {
         return max;
      }
      if (revRank >= totalWeight) {
         return min;
      }
      return quantile(1.0 - (double) revRank / totalWeight);
   }

   /**
    * Returns the trimmed mean between the given fractions.
    */
   public double trimmedMean(double lowFraction, double highFraction) {
      if (totalWeight == 0 || centroids.isEmpty()) {
         return Double.NaN;
      }
      if (lowFraction >= highFraction) {
         return Double.NaN;
      }

      List<Centroid> sorted = new ArrayList<>(centroids);
      sorted.sort(Comparator.comparingDouble(c -> c.mean));

      double lowWeight = lowFraction * totalWeight;
      double highWeight = highFraction * totalWeight;
      double sum = 0;
      double weight = 0;
      double cumulativeWeight = 0;

      for (Centroid c : sorted) {
         double nextCumulativeWeight = cumulativeWeight + c.weight;

         if (nextCumulativeWeight > lowWeight && cumulativeWeight < highWeight) {
            double effectiveStart = Math.max(cumulativeWeight, lowWeight);
            double effectiveEnd = Math.min(nextCumulativeWeight, highWeight);
            double effectiveWeight = effectiveEnd - effectiveStart;

            sum += c.mean * effectiveWeight;
            weight += effectiveWeight;
         }

         cumulativeWeight = nextCumulativeWeight;
         if (cumulativeWeight >= highWeight) {
            break;
         }
      }

      return weight > 0 ? sum / weight : Double.NaN;
   }

   /**
    * Merges another t-digest into this one.
    */
   public void merge(TDigest other) {
      // Preserve min/max from the source t-digest
      if (other.totalWeight > 0) {
         min = Math.min(min, other.min);
         max = Math.max(max, other.max);
      }
      for (Centroid c : other.centroids) {
         add(c.mean, c.weight);
      }
   }

   /**
    * Returns the number of observations.
    */
   public long count() {
      return totalWeight;
   }

   /**
    * Centroid class for storing mean and weight.
    */
   @ProtoTypeId(ProtoStreamTypeIds.RESP_T_DIGEST_CENTROID)
   public static final class Centroid {
      final double mean;
      final long weight;

      @ProtoFactory
      public Centroid(double mean, long weight) {
         this.mean = mean;
         this.weight = weight;
      }

      @ProtoField(number = 1, defaultValue = "0")
      public double getMean() {
         return mean;
      }

      @ProtoField(number = 2, defaultValue = "1")
      public long getWeight() {
         return weight;
      }
   }
}
