package org.infinispan.stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Mocks;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedWriteBehindStreamIteratorTest")
public class DistributedWriteBehindStreamIteratorTest extends BaseSetupStreamIteratorTest {

   private boolean asyncStore;
   private boolean sharedStore;

   public DistributedWriteBehindStreamIteratorTest() {
      // Cache Mode is provided in factory methods
      super(false, null);
   }

   DistributedWriteBehindStreamIteratorTest async(boolean asyncStore) {
      this.asyncStore = asyncStore;
      return this;
   }

   DistributedWriteBehindStreamIteratorTest shared(boolean sharedStore) {
      this.sharedStore = sharedStore;
      return this;
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new DistributedWriteBehindStreamIteratorTest().async(true).shared(true).cacheMode(CacheMode.REPL_SYNC),
            new DistributedWriteBehindStreamIteratorTest().async(false).shared(true).cacheMode(CacheMode.REPL_SYNC),
            new DistributedWriteBehindStreamIteratorTest().async(true).shared(false).cacheMode(CacheMode.REPL_SYNC),
            new DistributedWriteBehindStreamIteratorTest().async(false).shared(false).cacheMode(CacheMode.REPL_SYNC),

            new DistributedWriteBehindStreamIteratorTest().async(true).shared(true).cacheMode(CacheMode.DIST_SYNC),
            new DistributedWriteBehindStreamIteratorTest().async(false).shared(true).cacheMode(CacheMode.DIST_SYNC),
            new DistributedWriteBehindStreamIteratorTest().async(true).shared(false).cacheMode(CacheMode.DIST_SYNC),
            new DistributedWriteBehindStreamIteratorTest().async(false).shared(false).cacheMode(CacheMode.DIST_SYNC),
      };
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), asyncStore, sharedStore);
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "asyncStore", "sharedStore");
   }

   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      DummyInMemoryStoreConfigurationBuilder dimscb = builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      if (sharedStore) {
         dimscb.shared(true);
      }
      if (asyncStore) {
         dimscb.storeName(getTestName())
               .async().enable();
      }
   }

   @DataProvider(name = "rehashAware")
   public Object[][] dataProvider() {
      return new Object[][]{
            {Boolean.TRUE}, {Boolean.FALSE}
      };
   }

   @Test(dataProvider = "rehashAware")
   public void testBackupSegmentsOptimizationWithWriteBehindStore(boolean rehashAware) {
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);

      RpcManager rpcManager = Mocks.replaceComponentWithSpy(cache1, RpcManager.class);

      for (Cache<Object, String> cache : this.<Object, String>caches(CACHE_NAME)) {
         MagicKey key = new MagicKey(cache);
         cache.put(key, key.toString());
      }

      // Remember that segment ownership is as {0, 1}, {1, 2}, {2, 1}

      CacheStream<Map.Entry<Object, String>> stream = cache1.entrySet().stream();
      if (!rehashAware) stream = stream.disableRehashAware();

      int invocationCount;
      if (cacheMode.isReplicated()) {
         Map<Object, String> entries = mapFromIterator(stream.iterator());
         assertEquals(caches(CACHE_NAME).size(), entries.size());
         invocationCount = cacheManagers.size() - 1;
      } else {
         // Distributed cache
         // Cache1 owns 1 (primary) and 2 (backup)
         // When it is a write behind shared store it will have to go remote otherwise will stay local
         Map<Object, String> entries = mapFromIterator(stream.filterKeySegments(IntSets.immutableSet(2)).iterator());

         assertEquals(1, entries.size());
         invocationCount = 1;
      }
      // We can't stay local if we have a shared and async store - this is because write modifications are stored
      // on the primary owner, so we could miss updates
      if (asyncStore && sharedStore) {
         verify(rpcManager, times(invocationCount)).invokeCommand(any(Address.class), any(InitialPublisherCommand.class), any(), any());
      } else {
         verify(rpcManager, never()).invokeCommand(any(Address.class), any(InitialPublisherCommand.class), any(), any());
      }
   }
}
