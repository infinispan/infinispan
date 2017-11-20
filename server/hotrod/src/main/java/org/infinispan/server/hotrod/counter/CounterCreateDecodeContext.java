package org.infinispan.server.hotrod.counter;

import static org.infinispan.counter.util.EncodeUtil.decodeStorage;
import static org.infinispan.counter.util.EncodeUtil.decodeType;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeByte;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeLong;
import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.readMaybeVInt;

import java.util.Optional;
import java.util.function.Consumer;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;

/**
 * A decode context for {@link HotRodOperation#COUNTER_CREATE} operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterCreateDecodeContext extends CounterDecodeContext {

   private static final Log log = LogFactory.getLog(CounterCreateDecodeContext.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private DecodeState decodeState = DecodeState.DECODE_FLAGS;
   private CounterConfiguration.Builder builder;

   public CounterConfiguration getConfiguration() {
      return builder.build();
   }

   @Override
   DecodeStep nextStep() {
      switch (decodeState) {
         case DECODE_DONE:
            return null;
         case DECODE_INITIAL_VALUE:
            return buffer -> decodeLong(buffer, this::setInitialValue);
         case DECODE_UPPER_BOUND:
            return buffer -> decodeLong(buffer, this::setUpperBound);
         case DECODE_LOWER_BOUND:
            return buffer -> decodeLong(buffer, this::setLowerBound);
         case DECODE_CONCURRENCY:
            return this::decodeConcurrencyLevel;
         case DECODE_FLAGS:
            return this::decodeFlags;
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   boolean trace() {
      return trace;
   }

   @Override
   Log log() {
      return log;
   }

   private boolean decodeLong(ByteBuf buffer, Consumer<Long> consumer) {
      Optional<Long> optValue = readMaybeLong(buffer);
      optValue.ifPresent(consumer);
      return !optValue.isPresent();
   }

   private void setInitialValue(long value) {
      builder.initialValue(value);
      decodeState = DecodeState.DECODE_DONE;
      logDecoded("initial-value", value);
   }

   private void setUpperBound(long value) {
      builder.upperBound(value);
      decodeState = DecodeState.DECODE_INITIAL_VALUE;
      logDecoded("upper-bound", value);
   }

   private void setLowerBound(long value) {
      builder.lowerBound(value);
      decodeState = DecodeState.DECODE_UPPER_BOUND;
      logDecoded("lower-bound", value);
   }

   private void setConcurrencyLevel(int value) {
      builder.concurrencyLevel(value);
      decodeState = DecodeState.DECODE_INITIAL_VALUE;
      logDecoded("concurrency-level", value);
   }

   private boolean decodeConcurrencyLevel(ByteBuf buffer) {
      Optional<Integer> optConcurrency = readMaybeVInt(buffer);
      optConcurrency.ifPresent(this::setConcurrencyLevel);
      return !optConcurrency.isPresent();
   }

   private void createBuilderAndSetStorage(byte flags) {
      createBuilder(flags);
      setStorage(flags);
   }

   private void setStorage(byte flags) {
      Storage storage = decodeStorage(flags);
      builder.storage(storage);
      logDecoded("storage", storage);
   }

   private void createBuilder(byte flags) {
      CounterType type = decodeType(flags);
      builder = CounterConfiguration.builder(type);
      logDecoded("counter-type", type);
      switch (type) {
         case WEAK:
            decodeState = DecodeState.DECODE_CONCURRENCY;
            break;
         case BOUNDED_STRONG:
            decodeState = DecodeState.DECODE_LOWER_BOUND;
            break;
         case UNBOUNDED_STRONG:
            decodeState = DecodeState.DECODE_INITIAL_VALUE;
            break;
         default:
            throw new IllegalStateException("Unknown flag " + flags);
      }
   }

   private boolean decodeFlags(ByteBuf buffer) {
      Optional<Byte> optFlags = readMaybeByte(buffer);
      optFlags.ifPresent(this::createBuilderAndSetStorage);
      return !optFlags.isPresent();
   }


   private enum DecodeState {
      DECODE_FLAGS,
      DECODE_INITIAL_VALUE,
      DECODE_UPPER_BOUND,
      DECODE_LOWER_BOUND,
      DECODE_CONCURRENCY,
      DECODE_DONE
   }

}
