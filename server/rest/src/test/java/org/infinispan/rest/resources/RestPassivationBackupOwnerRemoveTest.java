package org.infinispan.rest.resources;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestPassivationBackupOwnerRemoveTest")
public class RestPassivationBackupOwnerRemoveTest extends AbstractRestResourceTest {

   private static final String CACHE_NAME = "passivationCache";
   private static final String KEY = "testKey";
   private static final String VALUE = "testValue";
   private static final int MAX_COUNT = 5;

   @Override
   protected void defineCaches(EmbeddedCacheManager cm) {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.memory().maxCount(MAX_COUNT);
      cfg.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);

      cm.defineConfiguration(CACHE_NAME, cfg.build());
   }

   @Test
   public void testDeleteWithSkipReturnValuesOnBackupOwnerShouldNotLeaveOrphan() throws Exception {
      RestCacheClient cacheClient = client.cache(CACHE_NAME);
      RestEntity valueEntity = RestEntity.create(MediaType.TEXT_PLAIN, VALUE);

      RestResponse putResponse = join(cacheClient.put(KEY, valueEntity));
      ResponseAssertion.assertThat(putResponse).isOk();

      RestResponse getResponse = join(cacheClient.get(KEY));
      ResponseAssertion.assertThat(getResponse).isOk();
      ResponseAssertion.assertThat(getResponse).hasReturnedText(VALUE);

      EmbeddedCacheManager node0 = cacheManagers.get(0);
      EmbeddedCacheManager node1 = cacheManagers.get(1);

      Cache<Object, Object> cache0 = node0.getCache(CACHE_NAME);
      Cache<Object, Object> cache1 = node1.getCache(CACHE_NAME);

      Object storageKey = cache0.getAdvancedCache().getDataContainer().iterator().next().getKey();
      DistributionInfo distribution = cache0.getAdvancedCache().getDistributionManager()
            .getCacheTopology().getDistribution(storageKey);

      boolean node0IsPrimary = distribution.isPrimary();
      Cache<Object, Object> primaryCache = node0IsPrimary ? cache0 : cache1;
      Cache<Object, Object> backupCache = node0IsPrimary ? cache1 : cache0;
      int backupNodeIndex = node0IsPrimary ? 1 : 0;
      int primaryNodeIndex = node0IsPrimary ? 0 : 1;

      // Trigger automatic passivation by filling the cache beyond maxCount
      // This will passivate our original key
      for (int i = 0; i < MAX_COUNT * 2; i++) {
         RestEntity filler = RestEntity.create(MediaType.TEXT_PLAIN, "filler" + i);
         try (RestResponse response = join(cacheClient.put("filler" + i, filler))) {
            ResponseAssertion.assertThat(response).isOk();
         }
      }

      // Wait until the entry is passivated and not in the data container before proceeding
      DummyInMemoryStore<?, ?> primaryStore = TestingUtil.extractComponent(primaryCache, PersistenceManager.class)
            .getStores(DummyInMemoryStore.class).iterator().next();
      DummyInMemoryStore<?, ?> backupStore = TestingUtil.extractComponent(backupCache, PersistenceManager.class)
            .getStores(DummyInMemoryStore.class).iterator().next();

      eventually(() -> primaryStore.contains(storageKey) && backupStore.contains(storageKey));
      assertThat(primaryCache.getAdvancedCache().getDataContainer().peek(storageKey)).isNull();
      assertThat(backupCache.getAdvancedCache().getDataContainer().peek(storageKey)).isNull();

      // Create REST clients targeting specific nodes
      try (RestClient primaryClient = RestClient.forConfiguration(getClientConfigForServer("user", "user", primaryNodeIndex).build());
           RestClient backupClient = RestClient.forConfiguration(getClientConfigForServer("user", "user", backupNodeIndex).build())) {

         RestCacheClient primaryCacheClient = primaryClient.cache(CACHE_NAME);
         RestCacheClient backupCacheClient = backupClient.cache(CACHE_NAME);

         // Submit the delete operation from the backup node.
         // The operation ignores the return values.
         Map<String, String> headers = new HashMap<>();
         headers.put("flags", "IGNORE_RETURN_VALUES");
         RestResponse deleteResponse = join(backupCacheClient.remove(KEY, headers));
         ResponseAssertion.assertThat(deleteResponse).isOk();

         // Assert it doesn't exist in the primary and the backup.
         RestResponse verifyFromPrimary = join(primaryCacheClient.get(KEY));
         ResponseAssertion.assertThat(verifyFromPrimary).doesntExist();

         RestResponse verifyFromBackup = join(backupCacheClient.get(KEY));
         ResponseAssertion.assertThat(verifyFromBackup).doesntExist();
      }

      // Sanity check on the data container
      assertThat(backupCache.getAdvancedCache().getDataContainer().peek(storageKey)).isNull();
      assertThat(backupStore.contains(storageKey)).isFalse();
      assertThat(primaryStore.contains(storageKey)).isFalse();
   }
}
