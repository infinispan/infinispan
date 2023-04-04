package org.infinispan.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.AbstractDelegatingInternalDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Publisher;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.SizeOptimizationTests")
public class SizeOptimizationTests extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "SizeOptimizationsTest";
   private static final int ENTRIES_SIZE = 42;
   private Optimization optimization;

   public SizeOptimizationTests optimization(Optimization optimization) {
      this.optimization = optimization;
      return this;
   }

   enum Optimization {
      /**
       * If the store is not asynchronous and is shared we can optimize to directly read the store size.
       * Using this optimization, the default implementation should never be used.
       */
      SHARED {
         @Override
         public ConfigurationBuilder configure(ConfigurationBuilder builder) {
            builder.persistence()
                  .passivation(false)
                  .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                     .storeName(name())
                     .shared(true)
                  .async()
                     .disable();
            return builder;
         }

         @Override
         public void verify(Cache<Object, Object> cache, SizeOptimizationTests test) throws Exception {
            neverCallDefault(cache);
            replaceDataContainerNotIterable(cache);

            assertEquals(cache.size(), ENTRIES_SIZE);
            int halfEntries = ENTRIES_SIZE / 2;

            for (int i = 0; i < halfEntries; i++) {
               cache.remove("key-" + i);
            }

            assertEquals(cache.size(), halfEntries);
         }
      },

      /**
       * If the store is private, segmented, and we have the CACHE_MODE_LOCAL flag set, then we can optimize
       * to directly call the container size. Using this optimization the default implementation should never be used.
       */
      SEGMENTED {
         @Override
         public ConfigurationBuilder configure(ConfigurationBuilder builder) {
            builder.persistence()
                  .passivation(false)
                  .addStore(DummyInMemoryStoreConfigurationBuilder.class)
                     .storeName(name())
                     .shared(false)
                     .segmented(true)
                     .async().disable()
                  .clustering()
                     .hash().numSegments(3);
            return builder;
         }

         @Override
         public void verify(Cache<Object, Object> cache, SizeOptimizationTests test) {
            neverCallDefault(cache);
            final Cache<Object, Object> localCache = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);

            // We retrieve the entries using __only__ the local cache and use it to verify the size.
            // Do not use the constructor passing the original set because that will call the .size() method.
            Set<Object> beforeEntries = new HashSet<>(ENTRIES_SIZE);
            beforeEntries.addAll(localCache.entrySet());

            // Replace the container only when verifying the size.
            // We can not replace all because we retrieve the key set.
            brieflyReplaceDataContainerNotIterable(localCache, () -> {
               assertEquals(localCache.size(), beforeEntries.size());
               return null;
            });

            int halfEntries = ENTRIES_SIZE / 2;
            for (int i = 0; i < halfEntries; i++) {
               cache.remove("key-" + i);
            }

            Set<Object> afterEntries = new HashSet<>(halfEntries);
            afterEntries.addAll(localCache.entrySet());
            brieflyReplaceDataContainerNotIterable(localCache, () -> {
               assertEquals(localCache.size(), afterEntries.size());
               return null;
            });
         }
      },

      /**
       * If we are not using any store, do not have any entries that expire, and have the CACHE_MODE_LOCAL flag set,
       * we can directly call the container size method. Here the test is a little longer because we verify that the
       * optimization is called only when the entries with lifetime are removed.
       */
      NO_STORE {
         @Override
         public ConfigurationBuilder configure(ConfigurationBuilder builder) {
            builder.persistence()
                  .passivation(false)
                  .clearStores();
            builder.transaction()
                  .lockingMode(LockingMode.OPTIMISTIC)
                  .transactionManagerLookup(new EmbeddedTransactionManagerLookup());
            return builder;
         }

         @Override
         public void verify(Cache<Object, Object> cache, SizeOptimizationTests test) throws Exception {
            final CheckPoint checkPoint = new CheckPoint();
            final Cache<Object, Object> localCache = cache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL);
            waitDefaultCalledMaxTo(localCache, checkPoint, 1);

            // We do a cluster wide search here, since we have entries with expiration set, the optimization is
            // not triggered.
            Future<Void> verifySize = test.fork(() -> {
               // Execution in a transaction skip the distributed optimization.
               TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
               tm.begin();
               try {
                  cache.put("key-tx", "value-tx");
                  assertEquals(cache.size(), ENTRIES_SIZE + 1);
                  cache.remove("key-tx");
               } finally {
                  tm.rollback();
               }
            });

            checkPoint.awaitStrict("default_invoked_done_" + localCache, 10, TimeUnit.SECONDS);
            checkPoint.trigger("default_invoked_done_proceed_" + localCache, 1);
            verifySize.get(10, TimeUnit.SECONDS);

            for (int i = 0; i < ENTRIES_SIZE; i++) {
               if ((i & 1) == 1) {
                  localCache.remove("key-" + i);
               }
            }


            // We retrieve the entries using __only__ the local cache and use it to verify the size.
            // Do not use the constructor passing the original set because that will call the .size() method.
            Set<Object> entries = new HashSet<>(ENTRIES_SIZE / 2);
            entries.addAll(localCache.entrySet());
            replaceDataContainerNotIterable(cache);
            assertEquals(localCache.size(), entries.size());
         }
      },
      ;

      public abstract ConfigurationBuilder configure(ConfigurationBuilder builder);

      public abstract void verify(Cache<Object, Object> cache, SizeOptimizationTests test) throws Exception;
   }

   @Override public Object[] factory() {
      return Arrays.stream(Optimization.values())
            .map(o -> new SizeOptimizationTests().optimization(o))
            .toArray();
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[] { optimization };
   }

   @Override
   protected String[] parameterNames() {
      return new String[] { "optimization" };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      createClusteredCaches(3, CACHE_NAME, optimization.configure(builder));
   }

   public void testSizeReturnsCorrectly() throws Exception {
      final Cache<Object, Object> cache = cache(0, CACHE_NAME);

      for (int i = 0; i < ENTRIES_SIZE; i++) {
         if ((i & 1) == 1) {
            cache.put("key-" + i, "v" + i, 30, TimeUnit.SECONDS);
         } else {
            cache.put("key-" + i, "v" + i);
         }
      }

      optimization.verify(cache, this);
   }

   private static void waitDefaultCalledMaxTo(final Cache<?, ?> cache, final CheckPoint checkPoint, int maxCalls) {
      final AtomicInteger executionTimes = new AtomicInteger(maxCalls);
      createMocking(cache, original -> invocation -> {
         if (executionTimes.getAndDecrement() == 0) {
            throw new TestException("Called more than " + maxCalls + " times to " + invocation.getMethod().getName());
         }

         CompletionStage<Object> result = ((CompletionStage<Object>) original.answer(invocation));
         result.thenRun(() -> {
            checkPoint.trigger("default_invoked_done_" + cache, 1);
            try {
               checkPoint.awaitStrict("default_invoked_done_proceed_" + cache, 10, TimeUnit.SECONDS);
            } catch (InterruptedException | TimeoutException e) {
               throw new TestException(e);
            }
         });
         return result;
      });
   }

   private static void neverCallDefault(final Cache<?, ?> cache) {
      waitDefaultCalledMaxTo(cache, null, 0);
   }

   private static void createMocking(final Cache<?, ?> cache, Function<Answer<Object>, Answer<?>> forward) {
      ClusterPublisherManager<?, ?> cpm = TestingUtil.extractComponent(cache, ClusterPublisherManager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(cpm);
      ClusterPublisherManager<?, ?> mockCpm = mock(ClusterPublisherManager.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(forward.apply(forwardedAnswer))
            .when(mockCpm).keyReduction(anyBoolean(), any(), any(), any(), anyLong(), any(), any(), any());
      TestingUtil.replaceComponent(cache, ClusterPublisherManager.class, mockCpm, true);
   }

   private static void brieflyReplaceDataContainerNotIterable(final Cache<?, ?> cache, Callable<Void> callable) {
      IDCNotIterable controlled = new IDCNotIterable(cache);
      TestingUtil.replaceComponent(cache, InternalDataContainer.class, controlled, true);

      try {
         callable.call();
      } catch (Exception e) {
         throw new TestException("Failed on callable", e);
      } finally {
         TestingUtil.replaceComponent(cache, InternalDataContainer.class, controlled.current, true);
      }
   }

   private static void replaceDataContainerNotIterable(final Cache<?, ?> cache) {
      InternalDataContainer<?, ?> controlled = new IDCNotIterable(cache);
      TestingUtil.replaceComponent(cache, InternalDataContainer.class, controlled, true);
   }

   static class IDCNotIterable extends AbstractDelegatingInternalDataContainer {
      final InternalDataContainer<?, ?> current;

      IDCNotIterable(Cache<?, ?> cache) {
         this.current = TestingUtil.extractComponent(cache, InternalDataContainer.class);
      }

      @Override
      protected InternalDataContainer<?, ?> delegate() {
         return current;
      }

      @Override
      public Iterator<InternalCacheEntry<?, ?>> iterator() {
         throw new TestException("Should not call iterator");
      }

      @Override
      public Iterator<InternalCacheEntry<?, ?>> iterator(IntSet segments) {
         throw new TestException("Should not call iterator");
      }

      @Override
      public Iterator<InternalCacheEntry<?, ?>> iteratorIncludingExpired() {
         throw new TestException("Should not call iterator");
      }

      @Override
      public Iterator<InternalCacheEntry<?, ?>> iteratorIncludingExpired(IntSet segments) {
         throw new TestException("Should not call iterator");
      }

      @Override
      public Publisher<InternalCacheEntry> publisher(int segment) {
         throw new TestException("Should not call publisher");
      }

      @Override
      public Publisher<InternalCacheEntry> publisher(IntSet segments) {
         throw new TestException("Should not call publisher");
      }
   }
}
