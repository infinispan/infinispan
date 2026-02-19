package org.infinispan.server.resp.hll.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.protostream.descriptors.Type;

import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * The {@link org.infinispan.server.resp.hll.HyperLogLog} magic.
 * <p>
 * The compact representation can track up to 2<sup>64</sup> elements with an accurate cardinality estimation and occupy
 * only 12Kb of memory. At instantiation, is reserved a long array with length {@link #STORE_SIZE}. We use
 * P={@link #HLL_BUCKET_COUNT} bits to identify the bucket and Q={@link #HLL_MAX_CONSECUTIVE_ZEROES} bits to count the
 * consecutive zeroes. The Q number is representable with 6 bits. Therefore, a register has a width of 6 bits
 * ({@link #REGISTER_WIDTH}).
 * </p>
 *
 * <p>
 * In C, we could allocate the memory space and manipulate it as we want, but here in Java, we have to do some bit
 * twiddling magic to make the registers fit. In other words, each position in the array uses long with 64 bits, whereas
 * one register uses only 6 bits. Multiple registers are within a single array position or spread across two positions.
 * Some manipulation is necessary to retrieve the correct register value from the array.
 * </p>
 *
 * <p>
 * The basic idea is that with a 64-bit hash, the first P bits identify the bucket, and the remaining Q bits we use to
 * count the trailing zeroes, and in case all bits are 0, the count is Q + 1. The next step is to update the bucket value
 * if it is smaller than the current count. From the bucket number and some bit-shifting, we identify the register.
 * </p>
 *
 * <p><b>Credits</b></p>
 * This implementation takes inspiration from the Redis implementation [1], which is based [2]. The bit-shifting
 * manipulation has some basis in [3], which implements the original algorithm without optimizations.
 *
 * <p>
 * Our cardinality estimation uses the optimizations proposed in [2]. And this is the reason we keep an additional int
 * array. We use this approach as it is the same as Redis uses. This algorithm does not require a bias correction as the
 * Google implementation or the [3]. That is, it does not need empirical data to correct estimations.
 * </p>
 *
 * @see <a href="https://github.com/redis/redis/blob/unstable/src/hyperloglog.c/">[1] Redis HyperLogLog implementation.</a>
 * @see <a href="https://arxiv.org/pdf/1702.01284.pdf/">[2] New cardinality estimation algorithms for HyperLogLog sketches</a>
 * @see <a href="https://github.com/aggregateknowledge/java-hll/">[3] Aggregate Knowledge HyperLogLog implementation.</a>
 * @since 15.0
 * @author José Bolina
 */
@ThreadSafe
@ProtoTypeId(ProtoStreamTypeIds.RESP_HYPER_LOG_LOG_COMPACT)
public class CompactSet implements HLLRepresentation {

   // The number of bits to identify the bucket.
   // This is equivalent of Math.log2(HLL_BUCKET_TOTAL).
   static final int HLL_BUCKET_COUNT = 14;

   // The number of bits to count the number of consecutive zeroes.
   // Utilizing a long, we have 64 bits total. The first HLL_BUCKET_COUNT bits are for the bucket identification.
   // The remaining bits count the number of consecutive zeroes.
   static final int HLL_MAX_CONSECUTIVE_ZEROES = Long.SIZE - HLL_BUCKET_COUNT;
   static final int HLL_BUCKET_TOTAL = 1 << HLL_BUCKET_COUNT;
   static final int HLL_BUCKET_MASK = HLL_BUCKET_TOTAL - 1;

   // Comes from Eq. 13 of [2]. This is used during the cardinality estimation and is equivalent to: 0.5 / Math.log(2).
   static final double ALPHA_INF = 0.721347520444481703680;

   // The size of the actual register we use to store the count of zeroes.
   // The highest number we can have is `HLL_MAX_CONSECUTIVE_ZEROES + 1`, which is representable in 6 bits.
   static final byte REGISTER_WIDTH = (byte) (Long.SIZE - Long.numberOfLeadingZeros(HLL_MAX_CONSECUTIVE_ZEROES));

   // The maximum value a register can hold with the given size.
   static final byte MAX_REGISTER_ENTRY = (byte) (1L << REGISTER_WIDTH);

   // Mask to identify the correct register in the store position.
   static final byte SINGLE_REGISTER_MASK = (byte) (MAX_REGISTER_ENTRY - 1);

   // The total store size. This takes into account the splitted registers and the multiple registers per array position.
   // In total, this will occupy roughly 12Kb.
   static final int STORE_SIZE = ((REGISTER_WIDTH * HLL_BUCKET_TOTAL) + SINGLE_REGISTER_MASK) >>> REGISTER_WIDTH;

   @GuardedBy("this")
   private final long[] store;

   @GuardedBy("this")
   private final int[] multiplicity;

   private volatile byte minimum;

   public CompactSet() {
      this.store = new long[STORE_SIZE];

      // The multiplicity is utilized during the cardinality calculation. We can maintain it pre-calculated during
      // insertions instead of calculating it in the cardinality.
      // We call it `multiplicity` to adhere to what the work [2] calls it. In truth, this is a histogram.
      this.multiplicity = new int[HLL_MAX_CONSECUTIVE_ZEROES + 2];

      // Since it is just created, all registers has a size o 0.
      this.multiplicity[0] = HLL_BUCKET_TOTAL;
      this.minimum = 0b0;
   }

   @ProtoFactory
   CompactSet(Collection<Long> store, Collection<Integer> multiplicity, byte minimum) {
      this.store = store.stream().mapToLong(Long::longValue).toArray();
      this.multiplicity = multiplicity.stream().mapToInt(Integer::intValue).toArray();
      this.minimum = minimum;
   }

   void readSource(Set<Long> hashes) {
      for (long hash : hashes) {
         setRegister(hash);
      }
   }

   @Override
   public boolean set(byte[] data) {
      return setRegister(Util.hash(data));
   }

   /**
    * The cardinality estimation is based on [2] (see class doc).
    * <p>
    * This is an implementation of Algorithm 6 [2]. It uses the {@link #multiplicity} vector and the methods of
    * {@link #tau(double)} and {@link #sigma(double)}. Note that this does not need to iterate over the real
    * registers to make an estimation,
    * </p>
    *
    * @return The estimate cardinality of the set.
    */
   @Override
   public long cardinality() {
      double z;
      synchronized (this) {
         z = HLL_BUCKET_TOTAL * tau(1 - (double) multiplicity[HLL_MAX_CONSECUTIVE_ZEROES + 1] / HLL_BUCKET_TOTAL);

         for (int k = HLL_MAX_CONSECUTIVE_ZEROES + 1; k >= 1; k--) {
            z = 0.5 * (z + multiplicity[k]);
         }
         z = z + HLL_BUCKET_TOTAL * sigma((double) multiplicity[0] / HLL_BUCKET_TOTAL);
      }

      double v = ALPHA_INF * HLL_BUCKET_TOTAL * HLL_BUCKET_TOTAL / z;
      return Math.round(v);
   }

   /**
    * Implemented from Algorithm 6 [2].
    *
    * @param x: A double floating point precision value in [0:1] interval.
    */
   private static double sigma(double x) {
      if (x == 1) return Double.POSITIVE_INFINITY;

      double zPrime;
      double y = 1;
      double z = x;

      do {
         x *= x;
         zPrime = z;
         z += x * y;
         y += y;
      } while(z != zPrime);

      return z;
   }

   /**
    * Implemented from Algorithm 6 [2].
    *
    * @param x: A double floating point precision value in [0:1] interval.
    */
   private static double tau(double x) {
      if (x == 0 || x == 1) return 0;

      double y = 1;
      double z = 1 - x;
      double zPrime = z;
      do {
         x = Math.sqrt(x);
         zPrime = z;
         y = 0.5 * y;
         z = z - Math.pow(1- x, 2) * y;
      } while (z != zPrime);

      return z / 3;
   }

   private boolean setRegister(long hash) {
      // The sequence is the `HLL_MAX_CONSECUTIVE_ZEROES` most-significant bits. We remove the `HLL_BUCKET_COUNT`
      // least-significant bits that identify the bucket. This needs to use an unsigned operation.
      long seq = hash >>> HLL_BUCKET_COUNT;

      // We count the number 0 from right until reaching the first bit set. If we don't have any bit set on `seq`,
      // we say the next active bit is HLL_MAX_CONSECUTIVE_ZEROES + 1. That happens when seq == 0, we moved the `HLL_BUCKET_COUNT` LSB.
      // Safe cast as the number has at most `HLL_MAX_CONSECUTIVE_ZEROES + 1` bits, which is representable in a byte.
      byte k = (byte) (seq == 0
            ? HLL_MAX_CONSECUTIVE_ZEROES + 1
            : (1 + Long.numberOfTrailingZeros(seq)));

      // If the calculated k is smaller than the known minimum, means that it will not update anything.
      // We can abort early safely.
      if (k > minimum) {
         // The bucket is the `HLL_BUCKET_COUNT` least-significant bits.
         int bucket = (int) (hash & HLL_BUCKET_MASK);
         return setRegister(bucket, k);
      }
      return false;
   }

   /**
    * Beware, there be dragons.
    * <p>
    * This converts the {@param bucket} mapping into the correct register in the {@link #store}. If the new {@param value}
    * is greater than the stored value, we can proceed updating the value.
    * </p>
    * <p>
    * Additionally, this implements the update to the {@link #multiplicity} array as defined on Algorithm 7 [2]. We update
    * the histogram to reflect the count values and the {@link #minimum}, if necessary.
    * </p>
    *
    * @param bucket: The bucket identified by the hash P least-significant bits.
    * @param value: The count of consecutive zeroes.
    * @return if the register was updated or not.
    */
   private boolean setRegister(int bucket, byte value) {
      assert bucket < (1L << 29) : "Bucket is out of bound.";
      int index = bucket * REGISTER_WIDTH;

      // We have an array withs longs to store each register. Since REGISTER_WIDTH < 8, each can have a register
      // split in two bytes. We identify the start position in the array.
      int first = index >>> REGISTER_WIDTH;

      // We advance to the next position and divide by the register width to get a possible split.
      int second = (index + REGISTER_WIDTH - 1) >>> REGISTER_WIDTH;

      // How many bits to skip before reaching the actual register.
      // For example, we use 6 bits out of 64 bits, in which position is our value? From bit 0, 1?
      int offset = index & SINGLE_REGISTER_MASK;

      // The register is using two positions if the first and second indexes are different.
      boolean spilled = first != second;
      byte stored;

      synchronized (this) {
         if (spilled) {
            // The register value spans across two positions.
            // First halve, we use the remainder to skip unnecessary bits.
            // Second halve, we now skip (MAX_SIZE - remainder) bits to retrieve the remaining bits.
            // Then the | to append the two halves and a mask.
            stored = (byte) (((store[first] >>> offset) | (store[second] << (MAX_REGISTER_ENTRY - offset))) & SINGLE_REGISTER_MASK);
         } else {
            // The value is within a single register. We simply use the remainder and a mask.
            stored = (byte) ((store[first] >>> offset) & SINGLE_REGISTER_MASK);
         }

         // We only update in case the sequence of zeroes is greater than seen previously.
         if (value > stored) {
            // We update the multiplicity to skip the calculation during the cardinality.
            // This comes from Algorithm 7 in [2].
            multiplicity[stored] -= 1;
            multiplicity[value] += 1;
            if (stored == minimum) {
               while (multiplicity[minimum] == 0) {
                  minimum += 1;
               }
            }

            // Now we need to write the new value back. And, we need to store on both positions, if necessary.
            if (spilled) {
               // The value occupies two positions.
               // We need to clear the previous occupied bits and set with the new value.
               store[first] &= (1L << offset) - 1;
               store[first] |= (long) value << offset;

               // Now, reset the remaining bits and set the value.
               store[second] &= ~(SINGLE_REGISTER_MASK >>> (MAX_REGISTER_ENTRY - offset));
               store[second] |= (value >>> (MAX_REGISTER_ENTRY - offset));
            } else {
               // The register occupy only a single position. We just reset and write the value back.
               store[first] &= ~((long) SINGLE_REGISTER_MASK << offset);
               store[first] |= (long) value << offset;
            }
            return true;
         }
      }

      return false;
   }

   /**
    * Read the register value for the given bucket.
    *
    * @param bucket the bucket index (0 to HLL_BUCKET_TOTAL - 1).
    * @return the register value.
    */
   private byte getRegister(int bucket) {
      int index = bucket * REGISTER_WIDTH;
      int first = index >>> REGISTER_WIDTH;
      int second = (index + REGISTER_WIDTH - 1) >>> REGISTER_WIDTH;
      int offset = index & SINGLE_REGISTER_MASK;

      synchronized (this) {
         if (first != second) {
            return (byte) (((store[first] >>> offset) | (store[second] << (MAX_REGISTER_ENTRY - offset))) & SINGLE_REGISTER_MASK);
         }
         return (byte) ((store[first] >>> offset) & SINGLE_REGISTER_MASK);
      }
   }

   /**
    * Merge another CompactSet into this one by taking the maximum register value for each bucket.
    *
    * @param other the CompactSet to merge from.
    */
   public void merge(CompactSet other) {
      for (int i = 0; i < HLL_BUCKET_TOTAL; i++) {
         byte val = other.getRegister(i);
         if (val > 0) {
            setRegister(i, val);
         }
      }
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   List<Long> store() {
      return LongStream.of(store)
            .boxed()
            .collect(Collectors.toList());
   }

   @ProtoField(number = 2, collectionImplementation = ArrayList.class, type = Type.UINT32)
   Collection<Integer> multiplicity() {
      return IntStream.of(multiplicity)
            .boxed()
            .collect(Collectors.toList());
   }

   @ProtoField(number = 3, javaType = byte.class, defaultValue = "0", type = Type.UINT32)
   byte minimum() {
      return minimum;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CompactSet that = (CompactSet) o;
      synchronized (this) {
         synchronized (that) {
            return Arrays.equals(this.store, that.store);
         }
      }
   }

   @Override
   public int hashCode() {
      synchronized (this) {
         return Arrays.hashCode(store);
      }
   }
}
