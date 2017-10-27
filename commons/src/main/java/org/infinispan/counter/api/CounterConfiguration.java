package org.infinispan.counter.api;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;

/**
 * A counter configuration used to define counters cluster wide via {@link CounterManager#defineCounter(String,
 * CounterConfiguration)}.
 * <p>
 * The configuration must be built using {@link CounterConfiguration#builder(CounterType)}. Only {@link CounterType} is
 * required.
 *
 * @author Pedro Ruivo
 * @see CounterType
 * @since 9.0
 */
public class CounterConfiguration {

   public static final AdvancedExternalizer<CounterConfiguration> EXTERNALIZER = new Externalize();

   private final long initialValue;
   private final long upperBound;
   private final long lowerBound;
   private final int concurrencyLevel;
   private final CounterType type;
   private final Storage storage;

   private CounterConfiguration(long initialValue, long lowerBound, long upperBound, int concurrencyLevel,
         CounterType type, Storage storage) {
      this.initialValue = initialValue;
      this.upperBound = upperBound;
      this.lowerBound = lowerBound;
      this.concurrencyLevel = concurrencyLevel;
      this.type = type;
      this.storage = storage;
   }

   public static Builder builder(CounterType type) {
      return new Builder(Objects.requireNonNull(type));
   }

   public long initialValue() {
      return initialValue;
   }

   public long upperBound() {
      return upperBound;
   }

   public long lowerBound() {
      return lowerBound;
   }

   public CounterType type() {
      return type;
   }

   public int concurrencyLevel() {
      return concurrencyLevel;
   }

   public Storage storage() {
      return storage;
   }

   @Override
   public String toString() {
      return "CounterConfiguration{" +
            "initialValue=" + initialValue +
            ", upperBound=" + upperBound +
            ", lowerBound=" + lowerBound +
            ", concurrencyLevel=" + concurrencyLevel +
            ", type=" + type +
            ", storage=" + storage +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      CounterConfiguration that = (CounterConfiguration) o;

      return initialValue == that.initialValue &&
            upperBound == that.upperBound &&
            lowerBound == that.lowerBound &&
            concurrencyLevel == that.concurrencyLevel &&
            type == that.type &&
            storage == that.storage;
   }

   @Override
   public int hashCode() {
      int result = (int) (initialValue ^ (initialValue >>> 32));
      result = 31 * result + (int) (upperBound ^ (upperBound >>> 32));
      result = 31 * result + (int) (lowerBound ^ (lowerBound >>> 32));
      result = 31 * result + concurrencyLevel;
      result = 31 * result + type.hashCode();
      result = 31 * result + storage.hashCode();
      return result;
   }

   /**
    * The builder of {@link CounterConfiguration}.
    */
   public static class Builder {
      private final CounterType type;
      private long initialValue = 0;
      private long lowerBound = Long.MIN_VALUE;
      private long upperBound = Long.MAX_VALUE;
      private Storage storage = Storage.VOLATILE;
      private int concurrencyLevel = 16;

      private Builder(CounterType type) {
         this.type = type;
      }

      /**
       * Sets the initial value.
       * <p>
       * The default value is zero.
       *
       * @param initialValue the new initial value.
       */
      public Builder initialValue(long initialValue) {
         this.initialValue = initialValue;
         return this;
      }

      /**
       * Sets the lower bound (inclusive) of the counter.
       * <p>
       * Only for {@link CounterType#BOUNDED_STRONG} counters.
       * <p>
       * The default value is {@link Long#MIN_VALUE}.
       *
       * @param lowerBound the new lower bound.
       */
      public Builder lowerBound(long lowerBound) {
         this.lowerBound = lowerBound;
         return this;
      }

      /**
       * Sets the upper bound (inclusive) of the counter.
       * <p>
       * Only for {@link CounterType#BOUNDED_STRONG} counters.
       * <p>
       * The default value is {@link Long#MAX_VALUE}.
       *
       * @param upperBound the new upper bound.
       */
      public Builder upperBound(long upperBound) {
         this.upperBound = upperBound;
         return this;
      }

      /**
       * Sets the storage mode of the counter.
       * <p>
       * The default value is {@link Storage#VOLATILE}.
       *
       * @param storage the new storage mode.
       * @see Storage
       */
      public Builder storage(Storage storage) {
         this.storage = Objects.requireNonNull(storage);
         return this;
      }

      /**
       * Sets the concurrency level of the counter.
       * <p>
       * Only for {@link CounterType#WEAK}.
       * <p>
       * <p>
       * The concurrency level set the amount of concurrent updates that can happen simultaneous. It is trade-off
       * between the write performance and read performance. A higher value will allow more concurrent updates, however
       * it will take more time to compute the counter value.
       * <p>
       * The default value is 64.
       *
       * @param concurrencyLevel the new concurrency level.
       */
      public Builder concurrencyLevel(int concurrencyLevel) {
         this.concurrencyLevel = concurrencyLevel;
         return this;
      }

      /**
       * @return the {@link CounterConfiguration} with this configuration.
       */
      public CounterConfiguration build() {
         return new CounterConfiguration(initialValue, lowerBound, upperBound, concurrencyLevel, type, storage);
      }
   }

   private static class Externalize implements AdvancedExternalizer<CounterConfiguration> {

      @Override
      public Set<Class<? extends CounterConfiguration>> getTypeClasses() {
         return Collections.singleton(CounterConfiguration.class);
      }

      @Override
      public Integer getId() {
         return Ids.COUNTER_CONFIGURATION;
      }

      @Override
      public void writeObject(ObjectOutput output, CounterConfiguration object) throws IOException {
         MarshallUtil.marshallEnum(object.type, output);
         MarshallUtil.marshallEnum(object.storage, output);
         output.writeLong(object.initialValue);
         switch (object.type) {
            case BOUNDED_STRONG:
               output.writeLong(object.lowerBound);
               output.writeLong(object.upperBound);
               break;
            case WEAK:
               UnsignedNumeric.writeUnsignedInt(output, object.concurrencyLevel);
               break;
            default:
         }
      }

      @Override
      public CounterConfiguration readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         CounterType type = MarshallUtil.unmarshallEnum(input, CounterType::valueOf);
         Storage storage = MarshallUtil.unmarshallEnum(input, Storage::valueOf);
         long initialValue = input.readLong();
         long lowerBound = Long.MIN_VALUE;
         long upperBound = Long.MAX_VALUE;
         int concurrencyLevel = 16;
         //noinspection ConstantConditions
         switch (type) {
            case BOUNDED_STRONG:
               lowerBound = input.readLong();
               upperBound = input.readLong();
               break;
            case WEAK:
               concurrencyLevel = UnsignedNumeric.readUnsignedInt(input);
               break;
            default:
         }
         return new CounterConfiguration(initialValue, lowerBound, upperBound, concurrencyLevel, type, storage);
      }
   }

}
