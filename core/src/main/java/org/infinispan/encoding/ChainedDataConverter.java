package org.infinispan.encoding;

import java.util.Objects;

/**
 * A DataConverter implementation that chains two DataConverter instances together.
 * <p>
 * The conversion order is:
 * <ul>
 *    <li>toStorage: applies first converter, then second converter</li>
 *    <li>fromStorage: applies second converter, then first converter (reverse order)</li>
 * </ul>
 * <p>
 * This converter is not serializable and should only be used locally.
 *
 * @since 16.2
 */
public final class ChainedDataConverter implements DataConverter {

   private final DataConverter first;
   private final DataConverter second;

   public ChainedDataConverter(DataConverter first, DataConverter second) {
      this.first = Objects.requireNonNull(first, "first converter cannot be null");
      this.second = Objects.requireNonNull(second, "second converter cannot be null");
   }

   public DataConverter getFirst() {
      return first;
   }

   public DataConverter getSecond() {
      return second;
   }

   @Override
   public Object fromStorage(Object stored) {
      Object intermediate = second.fromStorage(stored);
      return first.fromStorage(intermediate);
   }

   @Override
   public Object toStorage(Object toStore) {
      Object intermediate = first.toStorage(toStore);
      return second.toStorage(intermediate);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ChainedDataConverter that = (ChainedDataConverter) o;
      return Objects.equals(first, that.first) && Objects.equals(second, that.second);
   }

   @Override
   public int hashCode() {
      return Objects.hash(first, second);
   }

   @Override
   public String toString() {
      return "ChainedDataConverter{" +
            "first=" + first +
            ", second=" + second +
            '}';
   }
}
