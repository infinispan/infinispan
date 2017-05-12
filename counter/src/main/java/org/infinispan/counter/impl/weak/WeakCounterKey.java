package org.infinispan.counter.impl.weak;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
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

   public static final AdvancedExternalizer<WeakCounterKey> EXTERNALIZER = new Externalizer();

   private final ByteString counterName;
   private final int index;

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

   private static class Externalizer implements AdvancedExternalizer<WeakCounterKey> {

      private Externalizer() {
      }

      @Override
      public Set<Class<? extends WeakCounterKey>> getTypeClasses() {
         return Collections.singleton(WeakCounterKey.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.WEAK_COUNTER_KEY;
      }

      @Override
      public void writeObject(ObjectOutput output, WeakCounterKey object) throws IOException {
         ByteString.writeObject(output, object.counterName);
         UnsignedNumeric.writeUnsignedInt(output, object.index);
      }

      @Override
      public WeakCounterKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new WeakCounterKey(ByteString.readObject(input), UnsignedNumeric.readUnsignedInt(input));
      }
   }

}
