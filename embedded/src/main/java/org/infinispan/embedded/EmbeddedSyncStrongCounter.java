package org.infinispan.embedded;

import static org.infinispan.commons.util.concurrent.CompletableFutures.uncheckedAwait;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncStrongCounter;
import org.infinispan.counter.api.StrongCounter;

/**
 * @since 15.0
 */
public class EmbeddedSyncStrongCounter implements SyncStrongCounter {
   private final Embedded embedded;
   private final StrongCounter counter;

   EmbeddedSyncStrongCounter(Embedded embedded, StrongCounter counter) {
      this.embedded = embedded;
      this.counter = counter;
   }

   @Override
   public String name() {
      return counter.getName();
   }

   @Override
   public SyncContainer container() {
      return embedded.sync();
   }

   @Override
   public long value() {
      return uncheckedAwait(counter.getValue());
   }

   @Override
   public long addAndGet(long delta) {
      return uncheckedAwait(counter.addAndGet(delta));
   }

   @Override
   public CompletableFuture<Void> reset() {
      return counter.reset();
   }

   @Override
   public AutoCloseable listen(Consumer<CounterEvent> listener) {
      return null;
   }

   @Override
   public long compareAndSwap(long expect, long update) {
      return uncheckedAwait(counter.compareAndSwap(expect, update));
   }

   @Override
   public long getAndSet(long value) {
      return uncheckedAwait(counter.getAndSet(value));
   }

   @Override
   public CounterConfiguration configuration() {
      return (CounterConfiguration) counter.getConfiguration();
   }
}
