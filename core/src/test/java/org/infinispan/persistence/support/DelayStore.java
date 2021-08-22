package org.infinispan.persistence.support;

import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Test store that can delay store operations (or their completion).
 *
 * @author Dan Berindei
 * @since 13.0
 */
public class DelayStore extends DummyInMemoryStore {
   private static final Log log = LogFactory.getLog(DelayStore.class);

   private final AtomicInteger delayBeforeModificationCount = new AtomicInteger();
   private final AtomicInteger delayAfterModificationCount = new AtomicInteger();
   private final AtomicInteger delayBeforeEmitCount = new AtomicInteger();
   private volatile CompletableFuture<Void> delayFuture = CompletableFutures.completedNull();

   public void delayBeforeModification(int count) {
      assertTrue(delayFuture.isDone());
      delayFuture = new CompletableFuture<>();
      delayBeforeModificationCount.set(count);
   }

   public void delayAfterModification(int count) {
      assertTrue(delayFuture.isDone());
      delayFuture = new CompletableFuture<>();
      delayAfterModificationCount.set(count);
   }

   public void delayBeforeEmit(int count) {
      assertTrue(delayFuture.isDone());
      delayFuture = new CompletableFuture<>();
      delayBeforeEmitCount.set(count);
   }

   public void endDelay() {
      CompletableFuture<Void> oldFuture = delayFuture;
      if (oldFuture.isDone())
         return;

      log.tracef("Resuming delayed store operations");
      delayFuture = CompletableFutures.completedNull();
      oldFuture.complete(null);
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry entry) {
      CompletionStage<Void> stage = CompletableFutures.completedNull();
      if (!delayFuture.isDone() && delayBeforeModificationCount.decrementAndGet() >= 0) {
         log.tracef("Delaying before write to %s", entry.getKey());
         stage = delayFuture;
      }

      stage = stage.thenCompose(__ -> super.write(segment, entry));

      if (!delayFuture.isDone() && delayAfterModificationCount.decrementAndGet() >= 0) {
         log.tracef("Delaying after write to %s", entry.getKey());
         stage = stage.thenCompose(ignore -> delayFuture.thenRun(() -> {
            log.tracef("Resuming write to %s", entry.getKey());
         }));
      }
      return stage;
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      CompletionStage<Boolean> stage = CompletableFutures.completedNull();
      if (!delayFuture.isDone() && delayBeforeModificationCount.decrementAndGet() >= 0) {
         log.tracef("Delaying before write to %s", key);
         stage = delayFuture.thenApply(__ -> true);
      }

      stage = stage.thenCompose(__ -> super.delete(segment, key));

      if (!delayFuture.isDone() && delayAfterModificationCount.decrementAndGet() >= 0) {
         log.tracef("Delaying after write to %s", key);
         stage = stage.thenCompose(removed -> delayFuture.thenApply(__ -> {
            log.tracef("Resuming write to %s", key);
            return removed;
         }));
      }
      return stage;
   }

   @Override
   public Flowable<MarshallableEntry> publishEntries(IntSet segments, Predicate filter, boolean fetchValue) {
      return super.publishEntries(segments, filter, fetchValue)
            .delay(me -> {
                     if (!delayFuture.isDone() && delayBeforeEmitCount.decrementAndGet() >= 0) {
                        return Completable.fromCompletionStage(delayFuture).toFlowable();
                     } else {
                        return Flowable.empty();
                     }
                  });
   }

   @BuiltBy(ConfigurationBuilder.class)
   @ConfigurationFor(DelayStore.class)
   public static class Configuration extends DummyInMemoryStoreConfiguration {

      public Configuration(AttributeSet attributes, AsyncStoreConfiguration async) {
         super(attributes, async);
      }
   }

   public static class ConfigurationBuilder extends DummyInMemoryStoreConfigurationBuilder {

      public ConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder);
      }

      @Override
      public Configuration create() {
         return new Configuration(attributes.protect(), async.create());
      }
   }
}
