package org.infinispan.anchored;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.anchored.configuration.AnchoredKeysConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.rehash.TestWriteOperation;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestDataSCI;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "anchored.AnchoredKeysOperationsTest")
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
      managerBuilder.defaultCacheName(CACHE_NAME).serialization().addContextInitializer(TestDataSCI.INSTANCE);
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
         if (op.getPreviousValue() != null) {
            originator.put(key, op.getPreviousValue());
         }
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
         cache.clear();
      }
   }
}
