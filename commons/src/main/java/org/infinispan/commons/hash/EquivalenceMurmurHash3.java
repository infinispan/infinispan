package org.infinispan.commons.hash;

import org.infinispan.commons.equivalence.Equivalence;

/**
 * Version of {@link MurmurHash3} that uses provided {@link org.infinispan.commons.equivalence.Equivalence}
 * to get {@link #hashCode()} on provided objects (but arrays).
 *
 * @author Radim Vansa &ltrvansa@redhat.com&gt;
 */
public class EquivalenceMurmurHash3 implements Hash {
   protected Equivalence equivalence;

   public EquivalenceMurmurHash3() {}

   public EquivalenceMurmurHash3(Equivalence equivalence) {
      this.equivalence = equivalence;
   }

   public void setEquivalence(Equivalence equivalence) {
      this.equivalence = equivalence;
   }

   @Override
   public int hash(byte[] payload) {
      return MurmurHash3.MurmurHash3_x64_32(payload, MurmurHash3.DEFAULT_SEED);
   }

   @Override
   public int hash(int integer) {
      return MurmurHash3.MurmurHash3_32_32(integer, MurmurHash3.DEFAULT_SEED);
   }

   @Override
   public int hash(Object o) {
      if (o instanceof byte[])
         return MurmurHash3.MurmurHash3_x64_32((byte[]) o, MurmurHash3.DEFAULT_SEED);
      else if (o instanceof long[])
         return MurmurHash3.MurmurHash3_x64_32((long[]) o, MurmurHash3.DEFAULT_SEED);
      else
         return MurmurHash3.MurmurHash3_32_32(equivalence.hashCode(o), MurmurHash3.DEFAULT_SEED);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof EquivalenceMurmurHash3)) return false;

      EquivalenceMurmurHash3 that = (EquivalenceMurmurHash3) o;

      return !(equivalence != null ? !equivalence.equals(that.equivalence) : that.equivalence != null);
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("EquivalenceMurmurHash3{");
      sb.append("equivalence=").append(equivalence);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public int hashCode() {
      return equivalence != null ? equivalence.hashCode() : 0;
   }
}
