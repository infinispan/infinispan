package org.infinispan.anchored;

import org.infinispan.anchored.configuration.AnchoredKeysConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "anchored.AnchoredKeysScalingTest")
@CleanupAfterMethod
@AbstractInfinispanTest.FeatureCondition(feature = "anchored-keys")
public class AnchoredKeysScalingTest extends AbstractAnchoredKeysTest {
   public static final String CACHE_NAME = "testCache";
   public static final String KEY_1 = "key1";
   public static final String KEY_2 = "key2";
   public static final String KEY_3 = "key3";
   public static final String VALUE_1 = "value1";
   public static final String VALUE_2 = "value2";

   private StorageType storageType;

   @Override
   public Object[] factory() {
      return new Object[]{
            new AnchoredKeysScalingTest().storageType(StorageType.OBJECT),
            new AnchoredKeysScalingTest().storageType(StorageType.BINARY),
            new AnchoredKeysScalingTest().storageType(StorageType.OFF_HEAP),
            };
   }

   public AnchoredKeysScalingTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   @Override
   protected void createCacheManagers() {
      addNode();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"storage"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{storageType};
   }

   private Address addNode() {
      GlobalConfigurationBuilder managerBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      managerBuilder.defaultCacheName(CACHE_NAME);
      managerBuilder.serialization().addContextInitializer(ControlledConsistentHashFactory.SCI.INSTANCE);

      ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
      cacheBuilder.clustering().cacheMode(CacheMode.REPL_SYNC).hash().numSegments(4);
      cacheBuilder.clustering().stateTransfer().awaitInitialTransfer(false);
      cacheBuilder.memory().storage(storageType);
      cacheBuilder.addModule(AnchoredKeysConfigurationBuilder.class).enabled(true);

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(managerBuilder, cacheBuilder);
      return manager.getAddress();
   }

   public void testMultipleJoinsAndLeave() {
      Address A = address(0);

      cache(0).put(KEY_1, VALUE_1);

      assertValue(KEY_1, VALUE_1);
      assertNoValue(KEY_2);

      assertLocation(KEY_1, A, VALUE_1);
      assertNoLocation(KEY_2);

      Address B = addNode();
      waitForClusterToForm();

      assertValue(KEY_1, VALUE_1);
      assertNoValue(KEY_2);

      cache(0).put(KEY_2, VALUE_1);

      assertValue(KEY_1, VALUE_1);
      assertValue(KEY_2, VALUE_1);
      assertNoValue(KEY_3);

      TestingUtil.waitForNoRebalance(caches());
      assertLocation(KEY_1, A, VALUE_1);
      assertLocation(KEY_2, B, VALUE_1);
      assertNoLocation(KEY_3);

      Address C = addNode();
      waitForClusterToForm();

      assertValue(KEY_1, VALUE_1);
      assertValue(KEY_2, VALUE_1);
      assertNoValue(KEY_3);

      cache(0).put(KEY_3, VALUE_1);

      assertValue(KEY_1, VALUE_1);
      assertValue(KEY_2, VALUE_1);
      assertValue(KEY_3, VALUE_1);

      TestingUtil.waitForNoRebalance(caches());
      assertLocation(KEY_1, A, VALUE_1);
      assertLocation(KEY_2, B, VALUE_1);
      assertLocation(KEY_3, C, VALUE_1);

      killMember(2);
      waitForClusterToForm();

      assertNoValue(KEY_3);
      assertLocation(KEY_3, C, null);

      cache(0).put(KEY_3, VALUE_2);

      assertValue(KEY_3, VALUE_2);
      assertLocation(KEY_3, B, VALUE_2);

      cache(0).put(KEY_1, VALUE_2);

      assertValue(KEY_1, VALUE_2);
      assertLocation(KEY_1, A, VALUE_2);
   }
}
