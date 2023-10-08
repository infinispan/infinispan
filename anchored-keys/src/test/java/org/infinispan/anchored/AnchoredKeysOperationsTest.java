package org.infinispan.anchored;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.anchored.configuration.AnchoredKeysConfigurationBuilder;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.op.TestWriteOperation;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;

@Test(groups = "functional", testName = "anchored.AnchoredKeysOperationsTest")
@AbstractInfinispanTest.FeatureCondition(feature = "anchored-keys")
public class AnchoredKeysOperationsTest extends AbstractAnchoredKeysTest {
   public static final String CACHE_NAME = "testCache";

   private StorageType storageType;
   private boolean serverMode;

   @Override
   public Object[] factory() {
      return new Object[]{
            new AnchoredKeysOperationsTest().storageType(StorageType.OBJECT),
            new AnchoredKeysOperationsTest().storageType(StorageType.BINARY),
            new AnchoredKeysOperationsTest().storageType(StorageType.HEAP).serverMode(true),
            };
   }

   @DataProvider
   public static Object[][] operations() {
      return new Object[][]{
            {TestWriteOperation.PUT_CREATE},
            {TestWriteOperation.PUT_OVERWRITE},
            {TestWriteOperation.PUT_IF_ABSENT},
            {TestWriteOperation.REPLACE},
            {TestWriteOperation.REPLACE_EXACT},
            {TestWriteOperation.REMOVE},
            {TestWriteOperation.REMOVE_EXACT},
            {TestWriteOperation.PUT_MAP_CREATE},
            // TODO Add TestWriteOperation enum values for compute/computeIfAbsent/computeIfPresent/merge
            };
   }

   public AnchoredKeysOperationsTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   private Object serverMode(boolean serverMode) {
      this.serverMode = serverMode;
      return this;
   }

   @Override
   protected void createCacheManagers() {
      addNode();
      addNode();
      addNode();
      waitForClusterToForm();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"storage", "server"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{storageType, serverMode ? "y" : null};
   }

   private Address addNode() {
      GlobalConfigurationBuilder managerBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      managerBuilder.defaultCacheName(CACHE_NAME).serialization().addContextInitializers(ControlledConsistentHashFactory.SCI.INSTANCE, TestDataSCI.INSTANCE);
      if (serverMode) {
         managerBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      }

      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      ControlledConsistentHashFactory.Replicated consistentHashFactory =
            new ControlledConsistentHashFactory.Replicated(new int[]{0, 1, 2});
      cacheBuilder.clustering().cacheMode(CacheMode.REPL_SYNC)
                  .hash().numSegments(3).consistentHashFactory(consistentHashFactory);
      cacheBuilder.clustering().stateTransfer().awaitInitialTransfer(false);
      cacheBuilder.memory().storage(storageType);
      cacheBuilder.addModule(AnchoredKeysConfigurationBuilder.class).enabled(true);

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(managerBuilder, cacheBuilder);
      return manager.getAddress();
   }


   @Test(dataProvider = "operations")
   public void testSingleKeyOperations(TestWriteOperation op) {
      AdvancedCache<Object, Object> originator = advancedCache(0);
      for (Cache<Object, Object> cache : caches()) {
         MagicKey key = new MagicKey(cache);
         op.insertPreviousValue(originator, key);
         Object returnValue = op.perform(originator, key);
         assertEquals(op.getReturnValue(), returnValue);
         assertValue(key, op.getValue());
         if (op.getValue() != null) {
            assertLocation(key, address(2), op.getValue());
         }
      }
   }

   public void testMultiKeyOperations() {
      List<MagicKey> keys = new ArrayList<>();
      Map<MagicKey, Object> data = new HashMap<>();
      for (int i = 0; i < caches().size(); i++) {
         MagicKey key = new MagicKey("key-" + i, cache(i));
         String value = "value-" + i;
         keys.add(key);
         data.put(key, value);
      }

      for (int i = 0; i < caches().size(); i++) {
         MagicKey missingKey = new MagicKey("missingkey" + i, cache(i));
         keys.add(missingKey);
      }


      for (Cache<Object, Object> cache : caches()) {
         cache.putAll(data);

         data.forEach(this::assertValue);
         data.forEach((key, value) -> assertLocation(key, address(2), value));
         assertEquals(data, cache.getAdvancedCache().getAll(data.keySet()));
         assertEquals(data.keySet(), cache.keySet());
         assertEquals(new HashSet<>(data.values()), new HashSet<>(cache.values()));
         assertEquals(data.size(), cache.size());
         assertEquals(data.size(), (long) CompletionStages.join(cache.sizeAsync()));

         assertEquals(data,
                      Flowable.fromPublisher(cache.entrySet().localPublisher(IntSets.immutableRangeSet(3)))
                              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                              .blockingGet());
         cache.clear();
      }
   }

   public void testClusteredListener() throws InterruptedException {
      ClusteredListener listener = new ClusteredListener();
      cache(0).addListener(listener);
      for (Cache<Object, Object> originator : caches()) {
         String key = "key_" + originator.getCacheManager().getAddress();
         String value1 = "value-1";
         String value2 = "value-2";
         assertNull(originator.put(key, value1));
         assertValue(key, value1);
         assertTrue(originator.replace(key, value1, value2));
         assertValue(key, value2);
         assertEquals(value2, originator.remove(key));

         CacheEntryEvent<Object, Object> createEvent = listener.pollEvent();
         assertTrue(createEvent instanceof CacheEntryCreatedEvent);
         assertEquals(key, createEvent.getKey());
         assertEquals(value1, createEvent.getValue());

         CacheEntryEvent<Object, Object> replaceEvent = listener.pollEvent();
         assertTrue(replaceEvent instanceof CacheEntryModifiedEvent);
         assertEquals(key, replaceEvent.getKey());
         assertEquals(value2, replaceEvent.getValue());

         CacheEntryEvent<Object, Object> removeEvent = listener.pollEvent();
         assertTrue(removeEvent instanceof CacheEntryRemovedEvent);
         assertEquals(key, removeEvent.getKey());
         assertNull(removeEvent.getValue());
         // TODO The previous value is not always populated, because it's not stored in the context (see ISPN-5665)
         //  Instead ReplicationLogic.commitSingleEntry reads the previous value directly from the data container
//         assertEquals(value2, ((CacheEntryRemovedEvent<Object, Object>) removeEvent).getOldValue());

         assertFalse(listener.hasMoreEvents());
      }

   }

   @Listener(clustered = true)
   public class ClusteredListener {
      private final BlockingQueue<CacheEntryEvent<Object, Object>> events = new LinkedBlockingDeque<>();

      @CacheEntryCreated @CacheEntryModified @CacheEntryRemoved
      public void onEntryEvent(CacheEntryEvent<Object, Object> e) {
         log.tracef("Received event %s", e);
         events.add(e);
      }

      public boolean hasMoreEvents() throws InterruptedException {
         return events.poll(10, TimeUnit.MILLISECONDS) != null;
      }

      public CacheEntryEvent<Object, Object> pollEvent() throws InterruptedException {
         return events.poll(10, TimeUnit.SECONDS);
      }
   }
}
