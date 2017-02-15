package org.infinispan.statetransfer;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.StateTransferCacheLoaderFunctionalTest")
public class StateTransferCacheLoaderFunctionalTest extends StateTransferFunctionalTest {
   int id;
   ThreadLocal<Boolean> sharedCacheLoader = new ThreadLocal<Boolean>() {
      protected Boolean initialValue() {
         return false;
      }
   };

   public StateTransferCacheLoaderFunctionalTest() {
      super("nbst-with-loader");
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(String cacheName) {
      configurationBuilder.persistence().clearStores();
      // increment the DIMCS store id
      DummyInMemoryStoreConfigurationBuilder dimcs = new DummyInMemoryStoreConfigurationBuilder(configurationBuilder.persistence());
      dimcs.storeName("store number " + id++);
      dimcs.fetchPersistentState(true).shared(sharedCacheLoader.get()).preload(true);
      configurationBuilder.persistence().addStore(dimcs);
      configurationBuilder.persistence();

      return super.createCacheManager(cacheName);
   }

   @Override
   protected void writeInitialData(final Cache<Object, Object> c) {
      super.writeInitialData(c);
      c.evict(A_B_NAME);
      c.evict(A_B_AGE);
      c.evict(A_C_NAME);
      c.evict(A_C_AGE);
      c.evict(A_D_NAME);
      c.evict(A_D_AGE);
   }

   protected void verifyInitialDataOnLoader(Cache<Object, Object> c) throws Exception {
      CacheLoader l = TestingUtil.getFirstLoader(c);
      assert l.contains(A_B_AGE);
      assert l.contains(A_B_NAME);
      assert l.contains(A_C_AGE);
      assert l.contains(A_C_NAME);
      assert l.load(A_B_AGE).getValue().equals(TWENTY);
      assert l.load(A_B_NAME).getValue().equals(JOE);
      assert l.load(A_C_AGE).getValue().equals(FORTY);
      assert l.load(A_C_NAME).getValue().equals(BOB);
   }

   protected void verifyNoData(Cache<Object, Object> c) {
      assert c.isEmpty() : "Cache should be empty!";
   }

   protected void verifyNoDataOnLoader(Cache<Object, Object> c) throws Exception {
      CacheLoader l = TestingUtil.getFirstLoader(c);
      assert !l.contains(A_B_AGE);
      assert !l.contains(A_B_NAME);
      assert !l.contains(A_C_AGE);
      assert !l.contains(A_C_NAME);
      assert !l.contains(A_D_AGE);
      assert !l.contains(A_D_NAME);
   }

   public void testSharedLoader() throws Exception {
      try {
         sharedCacheLoader.set(true);
         Cache<Object, Object> c1 = createCacheManager(cacheName).getCache(cacheName);
         writeInitialData(c1);

         // starting the second cache would initialize an in-memory state transfer but not a persistent one since the loader is shared
         Cache<Object, Object> c2 = createCacheManager(cacheName).getCache(cacheName);
         TestingUtil.blockUntilViewsReceived(60000, c1, c2);
         TestingUtil.waitForStableTopology(c1, c2);

         verifyInitialDataOnLoader(c1);
         verifyInitialData(c1);

         verifyNoDataOnLoader(c2);
         // There shouldn't be any data locally since there was no entries in memory and the shared loader doesn't
         // actually share entries
         verifyNoData(c2.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL));
      } finally {
         sharedCacheLoader.set(false);
      }
   }

   public void testInitialSlowPreload() throws Exception {
      // Test for ISPN-2495
      // Preload on cache on node 1 is slow and unfinished at the point, where cache on node 2 starts.
      // Node 2 requests state, got answer that no entries available. Since node 2 is not coordinator,
      // preload is ignored. At the end, node 1 contains REPL cache with all entries, node 2 has same cache without entries.
      try {
         sharedCacheLoader.set(true);
         EmbeddedCacheManager cm1 = createCacheManager(cacheName);
         Cache<Object, Object> cache1 = cm1.getCache(cacheName);
         verifyNoDataOnLoader(cache1);
         verifyNoData(cache1);

         // write initial data
         cache1.put("A", new DelayedUnmarshal());
         cache1.put("B", new DelayedUnmarshal());
         cache1.put("C", new DelayedUnmarshal());
         assertEquals(cache1.size(), 3);
         cm1.stop();

         // this cache is only used to start networking
         final ConfigurationBuilder defaultConfigurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);

         // now lets start cm and shortly after another cache manager
         final EmbeddedCacheManager cm2 = super.createCacheManager(cacheName);
         cm2.defineConfiguration("initialCache", defaultConfigurationBuilder.build());
         cm2.startCaches("initialCache");

         EmbeddedCacheManager cm3 = super.createCacheManager(cacheName);
         cm3.defineConfiguration("initialCache", defaultConfigurationBuilder.build());
         cm3.startCaches("initialCache");

         // networking is started and cluster has 2 members
         TestingUtil.blockUntilViewsReceived(60000, cm2.getCache("initialCache"), cm3.getCache("initialCache"));

         // now fork start of "slow" cache
         Thread worker = new Thread(){
            @Override
            public void run() {
               cm2.startCaches(cacheName);
            }
         };
         worker.start();
         // lets wait a bit, cache is started pon cm2, but preload is not finished
         TestingUtil.sleepThread(1000);

         // uncomment this to see failing test
         worker.join();

         // at this point node is not alone, so preload is not used
         // the start of the cache must be blocked until state transfer is finished
         cm3.startCaches(cacheName);
         assertEquals(cm3.getCache(cacheName).size(), 3);
      } finally {
         sharedCacheLoader.set(false);
      }
   }

   public static class DelayedUnmarshal implements Serializable, ExternalPojo {

      private static final long serialVersionUID = 1L;

      private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
         TestingUtil.sleepThread(2000);
         in.defaultReadObject();
      }

      private void writeObject(ObjectOutputStream out) throws IOException {
         out.defaultWriteObject();
      }
   }

}
