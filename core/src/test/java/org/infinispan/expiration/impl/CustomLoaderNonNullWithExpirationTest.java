package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.MarshalledEntryFactory;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.testng.annotations.Test;

/**
 * Test to verify that a loader that always returns a non null value
 * @author wburns
 * @since 9.4
 */
@Test(groups = "functional", testName = "expiration.impl.CustomLoaderNonNullWithExpirationTest")
public class CustomLoaderNonNullWithExpirationTest extends SingleCacheManagerTest {
   private ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence().addStore(SimpleLoaderConfigurationBuilder.class);
      // Effectively disabling reaper
      builder.expiration().wakeUpInterval(1, TimeUnit.DAYS);

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      cache = cm.getCache();
      return cm;
   }

   @BuiltBy(SimpleLoaderConfigurationBuilder.class)
   @ConfigurationFor(SimpleLoader.class)
   public static class SimpleLoaderConfiguration extends AbstractStoreConfiguration {

      public SimpleLoaderConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
            SingletonStoreConfiguration singletonStore) {
         super(attributes, async, singletonStore);
      }
   }

   public static class SimpleLoaderConfigurationBuilder extends AbstractStoreConfigurationBuilder<SimpleLoaderConfiguration, SimpleLoaderConfigurationBuilder> {

      public SimpleLoaderConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder, SimpleLoaderConfiguration.attributeDefinitionSet());
      }

      @Override
      public SimpleLoaderConfiguration create() {
         return new SimpleLoaderConfiguration(attributes.protect(), async.create(), singletonStore.create());
      }

      @Override
      public SimpleLoaderConfigurationBuilder self() {
         return this;
      }

   }

   public static class SimpleLoader<K, V> implements CacheLoader<K, V> {

      static final String VALUE = "some-value";

      private MarshalledEntryFactory<K, V> factory;
      private TimeService timeService;

      @Override
      public void init(InitializationContext ctx) {
         factory = ctx.getMarshalledEntryFactory();
         timeService = ctx.getTimeService();
      }

      @Override
      public MarshalledEntry<K, V> load(Object key) {

         Metadata metadata = new EmbeddedMetadata.Builder()
               .lifespan(1, TimeUnit.SECONDS).build();

         long now = timeService.wallClockTime();
         return factory.newMarshalledEntry(key, VALUE, new InternalMetadataImpl(metadata, now, now));
      }

      @Override
      public boolean contains(Object key) {
         return true;
      }

      @Override
      public void start() {

      }

      @Override
      public void stop() {

      }
   }

   public void testEntryExpired() {
      String key = "some-key";
      Object value = cache.get(key);
      assertEquals(SimpleLoader.VALUE, value);

      // Should expire the in memory entry
      timeService.advance(TimeUnit.SECONDS.toMillis(2));

      value = cache.get(key);
      assertEquals(SimpleLoader.VALUE, value);
   }

   public void testExpireAfterWrapping() {
      // Every time a get is invoked it increases time by 2 seconds - causing entry to expire
      cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(new BaseCustomAsyncInterceptor() {
         @Override
         public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
            timeService.advance(TimeUnit.SECONDS.toMillis(2));
            return super.visitGetKeyValueCommand(ctx, command);
         }
      }, EntryWrappingInterceptor.class);

      String key = "some-key";
      Object value = cache.get(key);
      assertEquals(SimpleLoader.VALUE, value);

      value = cache.get(key);
      assertEquals(SimpleLoader.VALUE, value);
   }

   public void testConcurrentReadExpiration() throws InterruptedException, TimeoutException, BrokenBarrierException,
         ExecutionException {
      AtomicBoolean blockFirst = new AtomicBoolean(true);
      CyclicBarrier barrier = new CyclicBarrier(2);

      String key = "some-key";

      // We block the first get attempt, which will have no entry in data container in EntryWrappingInterceptor
      // But after we unblock it, the data container will have an expired entry
      cache.getAdvancedCache().getAsyncInterceptorChain().addInterceptorAfter(new BaseCustomAsyncInterceptor() {
         @Override
         public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
            if (blockFirst.getAndSet(false)) {
               assertEquals(NullCacheEntry.getInstance(), ctx.lookupEntry(key));
               barrier.await(10, TimeUnit.SECONDS);
               barrier.await(10, TimeUnit.SECONDS);
            }
            return super.visitGetKeyValueCommand(ctx, command);
         }
      }, EntryWrappingInterceptor.class);

      // This method will block above
      Future<Void> future = fork(() -> assertEquals(SimpleLoader.VALUE, cache.get(key)));

      // Make sure forked thread is blocked after EntryWrappingInterceptor, but before CacheLoaderInterceptor
      barrier.await(10, TimeUnit.SECONDS);

      // Now we read the key which should load the value
      Object value = cache.get(key);
      assertEquals(SimpleLoader.VALUE, value);

      // This will cause it to expire
      timeService.advance(TimeUnit.SECONDS.toMillis(2));

      // Finally let the fork complete - should be fine
      barrier.await(10, TimeUnit.SECONDS);

      future.get(10, TimeUnit.SECONDS);
   }
}
