package org.infinispan.counter.api;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_CONFIGURATION)
public class CounterConfiguration {

   private final long initialValue;
   private final long upperBound;
   private final long lowerBound;
   private final int concurrencyLevel;
   private final CounterType type;
   private final Storage storage;

   @ProtoFactory
   CounterConfiguration(long initialValue, long lowerBound, long upperBound, int concurrencyLevel, CounterType type,
         Storage storage) {
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

   @ProtoField(number = 1, defaultValue = "0")
   public long initialValue() {
      return initialValue;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public long upperBound() {
      return upperBound;
   }

   @ProtoField(number = 2, defaultValue = "0")
   public long lowerBound() {
      return lowerBound;
   }

   @ProtoField(number = 5)
   public CounterType type() {
      return type;
   }

   @ProtoField(number = 4, defaultValue = "0")
   public int concurrencyLevel() {
      return concurrencyLevel;
   }

   @ProtoField(number = 6)
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

}
