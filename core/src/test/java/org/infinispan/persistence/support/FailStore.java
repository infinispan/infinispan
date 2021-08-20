package org.infinispan.persistence.support;

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
import org.infinispan.test.TestException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Test store that can fail store operations
 *
 * @author Dan Berindei
 * @since 13.0
 */
public class FailStore extends DummyInMemoryStore {
   private static final Log log = LogFactory.getLog(FailStore.class);

   private final AtomicInteger failModificationCount = new AtomicInteger();
   private final AtomicInteger failPublishCount = new AtomicInteger();

   public void failModification(int count) {
      failModificationCount.set(count);
   }

   public void failPublish(int count) {
      failPublishCount.set(count);
   }

   @Override
   public CompletionStage<Void> write(int segment, MarshallableEntry entry) {
      if (failModificationCount.decrementAndGet() >= 0) {
         log.tracef("Delaying before write to %s", entry.getKey());
         return CompletableFutures.completedExceptionFuture(new TestException("Simulated write failure"));
      }
      return super.write(segment, entry);
   }

   @Override
   public CompletionStage<Boolean> delete(int segment, Object key) {
      if (failModificationCount.decrementAndGet() >= 0) {
         log.tracef("Delaying before write to %s", key);
         return CompletableFutures.completedExceptionFuture(new TestException("Simulated write failure"));
      }
      return super.delete(segment, key);
   }

   @Override
   public Flowable<MarshallableEntry> publishEntries(IntSet segments, Predicate filter, boolean fetchValue) {
      if (failPublishCount.decrementAndGet() >= 0) {
         return Flowable.error(new TestException("Simulated subscribe failure"));
      }
      return super.publishEntries(segments, filter, fetchValue);
   }

   @BuiltBy(ConfigurationBuilder.class)
   @ConfigurationFor(FailStore.class)
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
