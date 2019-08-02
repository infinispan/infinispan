package org.infinispan.counter.impl.strong;

import java.util.Objects;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.util.ByteString;

/**
 * The key to store in the {@link org.infinispan.Cache} used by {@link StrongCounter}.
 *
 * @author Pedro Ruivo
 * @see StrongCounter
 * @since 9.0
 */
public class StrongCounterKey implements CounterKey {

   @ProtoField(number = 1)
   ByteString counterName;

   StrongCounterKey() {}

   StrongCounterKey(String counterName) {
      this(ByteString.fromString(counterName));
   }

   private StrongCounterKey(ByteString counterName) {
      this.counterName = Objects.requireNonNull(counterName);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      StrongCounterKey that = (StrongCounterKey) o;

      return counterName.equals(that.counterName);

   }

   @Override
   public int hashCode() {
      return counterName.hashCode();
   }

   @Override
   public String toString() {
      return "CounterKey{" +
            "counterName=" + counterName +
            '}';
   }

   @Override
   public ByteString getCounterName() {
      return counterName;
   }
}
