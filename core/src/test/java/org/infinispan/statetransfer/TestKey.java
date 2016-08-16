package org.infinispan.statetransfer;

import java.io.Serializable;
import java.util.Random;

import org.infinispan.distribution.ch.ConsistentHash;

/**
 * A key that maps to a given data segment of the ConsistentHash.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
final class TestKey implements Serializable {

   private static final long serialVersionUID = -42;

   /**
    * A name used for easier debugging. This is not relevant for equals() and hashCode().
    */
   private final String name;

   /**
    * A carefully crafted hash code.
    */
   private final int hashCode;

   public TestKey(String name, int segmentId, ConsistentHash ch) {
      if (segmentId < 0 || segmentId >= ch.getNumSegments()) {
         throw new IllegalArgumentException("segmentId is out of range");
      }
      this.name = name;

      Random rnd = new Random();
      Integer r;
      do {
         r = rnd.nextInt();
      } while (segmentId != ch.getSegment(r));

      hashCode = r;
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != TestKey.class) return false;
      TestKey other = (TestKey) o;
      return hashCode == other.hashCode;
   }

   @Override
   public String toString() {
      return "TestKey{name=" + name + ", hashCode=" + hashCode + '}';
   }
}