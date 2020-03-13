package org.infinispan.counter.util;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;

/**
 * Utility class to handle encoding or decoding counter's classes.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class EncodeUtil {

   /*
   00000000
        ||^- 1 WEAK, 0 STRONG
        |^-- 1 BOUNDED, 0 UNBOUNDED
        ^--- 1 PERSISTENT, 0 VOLATILE
    */
   private static final byte WEAK_COUNTER = 0x01;
   private static final byte BOUNDED_COUNTER = 0x02;
   private static final byte UNBOUNDED_COUNTER = 0x00;

   private EncodeUtil() {
   }

   /**
    * Decodes the {@link CounterType}.
    *
    * @return the decoded {@link CounterType}.
    * @see #encodeTypeAndStorage(CounterConfiguration)
    */
   public static Storage decodeStorage(byte flags) {
      return (flags & 0x04) == 0 ? Storage.VOLATILE : Storage.PERSISTENT;
   }

   /**
    * Decodes the {@link Storage}.
    *
    * @return the decoded {@link Storage}.
    * @see #encodeTypeAndStorage(CounterConfiguration)
    */
   public static CounterType decodeType(byte flags) {
      switch (flags & 0x03) {
         case WEAK_COUNTER:
            return CounterType.WEAK;
         case BOUNDED_COUNTER:
            return CounterType.BOUNDED_STRONG;
         case UNBOUNDED_COUNTER:
            return CounterType.UNBOUNDED_STRONG;
         default:
            throw CONTAINER.invalidCounterTypeEncoded();
      }
   }

   private static byte encodeStorage(Storage storage) {
      switch (storage) {
         case VOLATILE:
            return 0x00;
         case PERSISTENT:
            return 0x04;
         default:
            throw new IllegalStateException("Unknown storage mode: " + storage);
      }
   }

   private static byte encodeType(CounterType type) {
      switch (type) {
         case UNBOUNDED_STRONG:
            return UNBOUNDED_COUNTER;
         case BOUNDED_STRONG:
            return BOUNDED_COUNTER;
         case WEAK:
            return WEAK_COUNTER;
         default:
            throw new IllegalStateException();
      }
   }

   /**
    * Encodes the {@link Storage} and the {@link CounterType}.
    * <p>
    * See the documentation for further details about the encoding.
    * <p>
    *
    * @return the encoded {@link Storage} and the {@link CounterType}.
    * @see <a href="https://infinispan.org/docs/dev/user_guide/user_guide.html#counter-config-encode">Counter
    * Configuration Encoding</a>
    */
   public static byte encodeTypeAndStorage(CounterConfiguration configuration) {
      return (byte) (encodeType(configuration.type()) | encodeStorage(configuration.storage()));
   }

   /**
    * Encodes the configuration.
    * <p>
    * See the documentation for further details about the encoding.
    * <p>
    *
    * @see <a href="https://infinispan.org/docs/dev/user_guide/user_guide.html#counter-config-encode">Counter
    * Configuration Encoding</a>
    */
   public static void encodeConfiguration(CounterConfiguration configuration, Consumer<Byte> byteConsumer,
         LongConsumer longConsumer, IntConsumer intConsumer) {
      byteConsumer.accept(encodeTypeAndStorage(configuration));
      switch (configuration.type()) {
         case WEAK:
            intConsumer.accept(configuration.concurrencyLevel());
            break;
         case BOUNDED_STRONG:
            longConsumer.accept(configuration.lowerBound());
            longConsumer.accept(configuration.upperBound());
            break;
         case UNBOUNDED_STRONG:
            break;
         default:
            throw new IllegalStateException();
      }
      longConsumer.accept(configuration.initialValue());
   }

   /**
    * Decodes a {@link CounterConfiguration} encoded by {@link #encodeConfiguration(CounterConfiguration, Consumer,
    * LongConsumer, IntConsumer)}.
    *
    * @return the decoded {@link CounterConfiguration}.
    * @see #encodeConfiguration(CounterConfiguration, Consumer, LongConsumer, IntConsumer)
    */
   public static CounterConfiguration decodeConfiguration(Supplier<Byte> byteSupplier, LongSupplier longSupplier,
         IntSupplier intSupplier) {
      byte flags = byteSupplier.get();
      CounterType type = decodeType(flags);
      CounterConfiguration.Builder builder = CounterConfiguration.builder(type);
      builder.storage(decodeStorage(flags));
      switch (type) {
         case WEAK:
            builder.concurrencyLevel(intSupplier.getAsInt());
            break;
         case BOUNDED_STRONG:
            builder.lowerBound(longSupplier.getAsLong());
            builder.upperBound(longSupplier.getAsLong());
            break;
         case UNBOUNDED_STRONG:
         default:
            break;
      }
      builder.initialValue(longSupplier.getAsLong());
      return builder.build();
   }

}
