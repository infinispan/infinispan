package org.infinispan.counter.impl.strong;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * The key to store in the {@link org.infinispan.Cache} used by {@link StrongCounter}.
 *
 * @author Pedro Ruivo
 * @see StrongCounter
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.STRONG_COUNTER_KEY)
public class StrongCounterKey implements CounterKey {

   private final ByteString counterName;

   StrongCounterKey(String counterName) {
      this(ByteString.fromString(counterName));
   }

   @ProtoFactory
   StrongCounterKey(ByteString counterName) {
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
   @ProtoField(number = 1)
   public ByteString getCounterName() {
      return counterName;
   }
}
