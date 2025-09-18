package org.infinispan.server.resp.hll;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.hll.internal.CompactSet;
import org.infinispan.server.resp.hll.internal.ExplicitSet;
import org.infinispan.server.resp.hll.internal.HLLRepresentation;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.errorprone.annotations.ThreadSafe;

/**
 * The HyperLogLog algorithm implementation.
 *
 * <p>
 * The HyperLogLog is a probabilistic algorithm for cardinality estimation by Flajolet et al. [1]. As with other
 * probabilistic data structures, the algorithm trades space by accuracy. It trades the memory necessary to keep track
 * of unique items by using "short bytes" to count elements.
 * </p>
 *
 * <p>
 * The version implemented here takes inspiration from the version implemented in Redis [2]. Follows part of the Google
 * paper [3], which makes a practical implementation, and the work on [4], with optimizations to the estimation accuracy.
 * </p>
 *
 * <h2>How it works</h2>
 *
 * From a bird's eye view, the algorithm uses a hash function that returns a value with (P + Q) bits. The P bits identify
 * one bucket to place the count, and the Q we extract the number of consecutive trailing zeroes. Since the higher the
 * number of zeroes, the lower the probability (N + 1 zeroes is half the N), the algorithm then collects the values from
 * all the buckets to guess the cardinality.
 *
 * <h2>The implementation</h2>
 *
 * We use the {@link org.infinispan.commons.hash.MurmurHash3} with 64 bits. Therefore, we have <code>P=14</code> for the
 * number of buckets and <code>Q=64-P</code> to count the number of trailing zeroes. In theory, this allows us to
 * count up to 2<sup>64</sup> elements!
 *
 * <p>
 * We use two representations to store the elements. The first one, for small cardinalities (0 - 192), uses the explicit
 * hash return and a hash set, as seen on {@link ExplicitSet}. The compact representation with
 * {@link CompactSet}. The form the algorithm proposes with the "short bytes" and
 * occupies low memory.
 * </p>
 *
 * <p>
 * The compact representation occupies roughly 12Kb and could count up to 2<sup>64</sup> elements. The explicit representation
 * counts up to 192 because it starts using more than 12Kb of memory. After reaching the threshold, the representation
 * changes from explicit to compact.
 * </p>
 *
 * @see <a href="https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf/">[1] HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm</a>
 * @see <a href="http://antirez.com/news/75">[2] Redis new data structure: the HyperLogLog</a>
 * @see <a href="http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf/">[3] HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm</a>
 * @see <a href="https://arxiv.org/pdf/1702.01284.pdf/">[4] New cardinality estimation algorithms for HyperLogLog sketches</a>
 * @since 15.0
 * @author José Bolina
 */
@ThreadSafe
@ProtoTypeId(ProtoStreamTypeIds.RESP_HYPER_LOG_LOG)
public class HyperLogLog {

   @GuardedBy("this")
   private ExplicitSet explicit;

   @GuardedBy("this")
   private CompactSet compact;

   public HyperLogLog() { }

   @ProtoFactory
   HyperLogLog(ExplicitSet explicit, CompactSet compact) {
      this.explicit = explicit;
      this.compact = compact;
   }

   public boolean add(byte[] data) {
      ExplicitSet src = null;
      boolean ret;
      synchronized (this) {
         if (compact != null) return compact.set(data);
         if (explicit == null) explicit = new ExplicitSet();

         try {
            ret = explicit.set(data);
         } finally {
            // Changes from the explicit to the compact representation once the threshold is reached.
            if (explicit.needsMigration()) {
               src = explicit;
               explicit = null;
               compact = new CompactSet();
            }
         }
      }

      // Migrate outside the synchronized block. Both representations are thread-safe.
      if (src != null) src.migrate(compact);
      return ret;
   }

   public long cardinality() {
      HLLRepresentation representation = store();
      return representation == null ? 0 : representation.cardinality();
   }

   synchronized HLLRepresentation store() {
      if (compact != null) return compact;
      if (explicit != null) return explicit;
      return null;
   }


   @ProtoField(number = 1)
   ExplicitSet explicit() {
      return explicit;
   }

   @ProtoField(number = 2)
   CompactSet compact() {
      return compact;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      HyperLogLog that = (HyperLogLog) o;
      synchronized (this) {
         synchronized (that) {
            return Objects.equals(this.store(), that.store());
         }
      }
   }

   @Override
   public int hashCode() {
      synchronized (this) {
         return Objects.hash(store());
      }
   }
}
