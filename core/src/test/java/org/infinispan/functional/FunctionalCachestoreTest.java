package org.infinispan.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.api.functional.EntryView.ReadWriteEntryView;
import org.infinispan.commons.api.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.commons.api.functional.Param;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt; && Krzysztof Sobolewski &lt;Krzysztof.Sobolewski@atende.pl&gt;
 */
@Test(groups = "functional", testName = "functional.FunctionalCachestoreTest")
public class FunctionalCachestoreTest extends AbstractFunctionalTest {
   @DataProvider(name = "booleans")
   public static Object[][] booleans() {
      return new Object[][] { { Boolean.TRUE }, { Boolean.FALSE } };
   }

   private boolean tx;
   private ReadWriteMap<Object, String> rw;

   public FunctionalCachestoreTest() {
      this.cleanup = CleanupPhase.AFTER_METHOD;
   }

   private static Address getAddress(Cache<Object, Object> cache) {
      return cache.getAdvancedCache().getRpcManager().getAddress();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(4, new ConfigurationBuilder());
      // Create distributed caches
      ConfigurationBuilder distBuilder = new ConfigurationBuilder();
      // we want at least one non-primary owner
      distBuilder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2)
                 .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      if (tx) {
         distBuilder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      }
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(DIST, distBuilder.build()));
      // Create replicated caches
      ConfigurationBuilder replBuilder = new ConfigurationBuilder();
      replBuilder.clustering().cacheMode(CacheMode.REPL_SYNC)
                 .persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      if (tx) {
         replBuilder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      }
      cacheManagers.stream().forEach(cm -> cm.defineConfiguration(REPL, replBuilder.build()));
      // Wait for cluster to form
      waitForClusterToForm(DIST, REPL);
   }

   @Override
   @BeforeMethod
   public void createBeforeMethod() throws Throwable {
      /*
       * skip; will be called from the test methods (we need to initialize the
       * tx field somehow)
       */
   }

   private void createBeforeMethod(boolean tx) throws Throwable {
      this.tx = tx;
      super.createBeforeMethod();
      this.rw = ReadWriteMapImpl.create(fmapD1);
   }

   @Test(dataProvider = "booleans")
   public void testLoad(boolean isSourceOwner) throws Throwable {
      doTest(false, isSourceOwner);
   }

   @Test(dataProvider = "booleans", enabled = false /* ISPN-6573 */)
   public void testLoadTx(boolean isSourceOwner) throws Throwable {
      doTest(true, isSourceOwner);
   }

   private void doTest(boolean tx, boolean isSourceOwner) throws Throwable {
      createBeforeMethod(tx);

      Object key;
      if (isSourceOwner) {
         // this is simple: find a key that is local to the originating node
         key = getKeyForCache(0, DIST);
      } else {
         // this is more complicated: we need a key that is *not* local to the originating node
         key = IntStream.iterate(0, i -> i + 1)
               .mapToObj(i -> "key" + i)
               .filter(k -> !cache(0, DIST).getAdvancedCache().getDistributionManager().getLocality(k).isLocal())
               .findAny()
               .get();
      }
      List<Cache<Object, Object>> owners = caches(DIST).stream()
            .filter(cache -> cache.getAdvancedCache().getDistributionManager().getLocality(key).isLocal())
            .collect(Collectors.toList());
      TransactionManager tm = cache(0, DIST).getAdvancedCache().getTransactionManager();

      if (tx) {
         tm.begin();
      }
      rw.withParams(Param.FutureMode.COMPLETED).eval(key, (Function<ReadWriteEntryView<Object, String>, Object> & Serializable) view -> {
         assertFalse(view.find().isPresent());
         view.set("value");
         return null;
      }).get();
      if (tx) {
         tm.commit();
      }

      caches(DIST).forEach(cache -> assertEquals(cache.get(key), "value", getAddress(cache).toString()));
      caches(DIST).forEach(cache -> cache.evict(key));
      caches(DIST).forEach(cache -> assertFalse(cache.getAdvancedCache().getDataContainer().containsKey(key), getAddress(cache).toString()));
      owners.forEach(cache -> {
         Set<DummyInMemoryStore> stores = cache.getAdvancedCache().getComponentRegistry().getComponent(PersistenceManager.class).getStores(DummyInMemoryStore.class);
         DummyInMemoryStore store = stores.iterator().next();
         assertTrue(store.contains(key), getAddress(cache).toString());
      });

      if (tx) {
         tm.begin();
      }
      rw.withParams(Param.FutureMode.COMPLETED).eval(key, (Function<ReadWriteEntryView<Object, String>, Object> & Serializable) view -> {
         assertEquals(view.get(), "value");
         assertTrue(view.find().isPresent());
         return null;
      }).get();
      if (tx) {
         tm.commit();
      }
   }
}