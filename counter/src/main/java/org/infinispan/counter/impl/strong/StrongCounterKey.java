package org.infinispan.counter.impl.strong;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.util.ByteString;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The key to store in the {@link org.infinispan.Cache} used by {@link StrongCounter}.
 *
 * @author Pedro Ruivo
 * @see StrongCounter
 * @since 9.0
 */
public class StrongCounterKey implements CounterKey {

   public static final AdvancedExternalizer<StrongCounterKey> EXTERNALIZER = new Externalizer();

   private final ByteString counterName;

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

   private static class Externalizer implements AdvancedExternalizer<StrongCounterKey> {

      private Externalizer() {
      }

      @Override
      public Set<Class<? extends StrongCounterKey>> getTypeClasses() {
         return Collections.singleton(StrongCounterKey.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.STRONG_COUNTER_KEY;
      }

      @Override
      public void writeObject(ObjectOutput output, StrongCounterKey object) throws IOException {
         ByteString.writeObject(output, object.counterName);
      }

      @Override
      public StrongCounterKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new StrongCounterKey(ByteString.readObject(input));
      }
   }
}
