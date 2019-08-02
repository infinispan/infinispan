package org.infinispan.counter.impl.weak;

import java.util.Objects;

import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * The key to store in the {@link org.infinispan.Cache} used by {@link WeakCounter}.
 * <p>
 * The weak consistent counters splits the counter's value in multiple keys. This class contains the index.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class WeakCounterKey implements CounterKey {

   @ProtoField(number = 1)
   ByteString counterName;

   @ProtoField(number = 2, defaultValue = "0")
   int index;

   WeakCounterKey() {}

   WeakCounterKey(ByteString counterName, int index) {
      this.counterName = Objects.requireNonNull(counterName);
      this.index = requirePositive(index);
   }

   private static int requirePositive(int i) {
      if (i < 0) {
         throw new IllegalArgumentException("Requires positive index");
      }
      return i;
   }

   int getIndex() {
      return index;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      WeakCounterKey that = (WeakCounterKey) o;

      return index == that.index &&
            counterName.equals(that.counterName);

   }

   @Override
   public int hashCode() {
      int result = counterName.hashCode();
      result = 31 * result + index;
      return result;
   }

   @Override
   public String toString() {
      return "WeakCounterKey{" +
            "counterName=" + counterName +
            ", index=" + index +
            '}';
   }

   @Override
   public ByteString getCounterName() {
      return counterName;
   }
}
